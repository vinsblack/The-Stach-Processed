/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service.accord.interop;

import java.io.IOException;
import javax.annotation.Nullable;

import accord.api.Data;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.messages.ReadData;
import accord.messages.MessageType;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadRepairVerbHandler;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.accord.AccordMessageSink.AccordMessageType;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.IVersionedSerializer;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.serializers.ReadDataSerializers;
import org.apache.cassandra.service.accord.serializers.ReadDataSerializers.ReadDataSerializer;
import org.apache.cassandra.service.accord.serializers.Version;

/**
 * Applies a read repair mutation from inside the context of a CommandStore via AbstractExecute
 * ensuring that the contents of the read repair consist of data that isn't from transactions that
 * haven't been committed yet at this command store.
 */
public class AccordInteropReadRepair extends ReadData
{
    public static final IVersionedSerializer<AccordInteropReadRepair> requestSerializer = new ReadDataSerializer<AccordInteropReadRepair>()
    {
        @Override
        public void serialize(AccordInteropReadRepair repair, DataOutputPlus out, Version version) throws IOException
        {
            CommandSerializers.txnId.serialize(repair.txnId, out);
            KeySerializers.participants.serialize(repair.scope, out);
            out.writeUnsignedVInt(repair.executeAtEpoch);
            Mutation.serializer.serialize(repair.mutation, out, version.messageVersion());
        }

        @Override
        public AccordInteropReadRepair deserialize(DataInputPlus in, Version version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in);
            Participants<?> scope = KeySerializers.participants.deserialize(in);
            long executeAtEpoch = in.readUnsignedVInt();
            Mutation mutation = Mutation.serializer.deserialize(in, version.messageVersion());
            return new AccordInteropReadRepair(txnId, scope, executeAtEpoch, mutation);
        }

        @Override
        public long serializedSize(AccordInteropReadRepair repair, Version version)
        {
            return CommandSerializers.txnId.serializedSize(repair.txnId)
                   + KeySerializers.participants.serializedSize(repair.scope)
                   + TypeSizes.sizeofUnsignedVInt(repair.executeAtEpoch)
                   + Mutation.serializer.serializedSize(repair.mutation, version.messageVersion());
        }
    };

    static class ReadRepairCallback extends AccordInteropReadCallback<Object>
    {
        public ReadRepairCallback(Node.Id id, InetAddressAndPort endpoint, Message<?> message, RequestCallback<Object> wrapped, MaximalCommitSender maximalCommitSender)
        {
            super(id, endpoint, message, wrapped, maximalCommitSender);
        }

        @Override
        Object convertResponse(ReadOk ok)
        {
            return NoPayload.noPayload;
        }
    }

    private static final ExecuteOn EXECUTE_ON = new ExecuteOn(SaveStatus.ReadyToExecute, SaveStatus.Applied);

    private final Mutation mutation;

    private static final IVersionedSerializer<Data> noop_data_serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(Data t, DataOutputPlus out, Version version) throws IOException {}
        @Override
        public Data deserialize(DataInputPlus in, Version version) throws IOException { return Data.NOOP_DATA; }

        public long serializedSize(Data t, Version version) { return 0; }
    };

    public static final IVersionedSerializer<ReadReply> replySerializer = new ReadDataSerializers.ReplySerializer<>(noop_data_serializer);

    public AccordInteropReadRepair(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> scope, long executeAtEpoch, Mutation mutation)
    {
        super(to, topologies, txnId, scope, executeAtEpoch);
        this.mutation = mutation;
    }

    public AccordInteropReadRepair(TxnId txnId, Participants<?> scope, long executeAtEpoch, Mutation mutation)
    {
        super(txnId, scope, executeAtEpoch);
        this.mutation = mutation;
    }

    @Override
    protected ExecuteOn executeOn()
    {
        return EXECUTE_ON;
    }

    @Override
    public ReadType kind()
    {
        return ReadType.readTxnData;
    }

    @Override
    protected AsyncChain<Data> beginRead(SafeCommandStore safeStore, Timestamp executeAt, PartialTxn txn, Participants<?> execute)
    {
        // TODO (required): subtract unavailable ranges, either from read or from response (or on coordinator)
        return AsyncChains.ofCallable(Verb.READ_REPAIR_REQ.stage.executor(), () -> {
                                          ReadRepairVerbHandler.instance.applyMutation(mutation);
                                          return Data.NOOP_DATA;
                                      });
    }

    @Override
    protected ReadOk constructReadOk(Ranges unavailable, Data data, long uniqueHlc)
    {
        return new InteropReadRepairOk(unavailable, data, uniqueHlc);
    }

    @Override
    public MessageType type()
    {
        return AccordMessageType.INTEROP_READ_REPAIR_REQ;
    }

    private static class InteropReadRepairOk extends ReadOk
    {
        public InteropReadRepairOk(@Nullable Ranges unavailable, @Nullable Data data, long uniqueHlc)
        {
            super(unavailable, data, uniqueHlc);
        }

        @Override
        public MessageType type()
        {
            return AccordMessageType.INTEROP_READ_REPAIR_RSP;
        }
    }
}
