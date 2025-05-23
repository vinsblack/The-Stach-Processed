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
package org.apache.cassandra.db.virtual;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.execute;
import static org.apache.cassandra.schema.DistributedMetadataLogKeyspace.TABLE_NAME;
import static org.apache.cassandra.schema.SchemaConstants.METADATA_KEYSPACE_NAME;

public final class ClusterMetadataLogTable extends AbstractVirtualTable
{
    private static final String EPOCH = "epoch";
    private static final String KIND = "kind";
    private static final String TRANSFORMATION = "transformation";
    private static final String ENTRY_ID = "entry_id";
    private static final String ENTRY_TIME = "entry_time";

    ClusterMetadataLogTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "cluster_metadata_log")
                           .comment("cluster metadata log")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(MetaStrategy.partitioner)
                           .addPartitionKeyColumn(EPOCH, LongType.instance)
                           .addRegularColumn(KIND, UTF8Type.instance)
                           .addRegularColumn(TRANSFORMATION, UTF8Type.instance)
                           .addRegularColumn(ENTRY_ID, LongType.instance)
                           .addRegularColumn(ENTRY_TIME, TimestampType.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        for (Map.Entry<Long, Map<String, Object>> entry : log(Epoch.FIRST.getEpoch(), Long.MAX_VALUE).entrySet())
        {
            SimpleDataSet data = result.row(entry.getKey());
            for (Map.Entry<String, Object> rowEntry : entry.getValue().entrySet())
                data = data.column(rowEntry.getKey(), rowEntry.getValue());
        }
        return result;
    }

    public static Map<Long, Map<String, Object>> log(long startEpoch, long endEpoch)
    {
        try
        {
            Map<Long, Map<String, Object>> result = new LinkedHashMap<>();
            UntypedResultSet res = execute(format("SELECT epoch, kind, transformation, entry_id, writetime(kind) as wt " +
                                                  "FROM %s.%s WHERE token(epoch) >= token(?) AND token(epoch) <= token(?)", METADATA_KEYSPACE_NAME, TABLE_NAME), ConsistencyLevel.QUORUM, endEpoch, startEpoch);
            for (UntypedResultSet.Row r : res)
            {
                Transformation.Kind kind = Transformation.Kind.fromId(r.getInt("kind"));
                Transformation transformation = kind.fromVersionedBytes(r.getBlob("transformation"));
                Map<String, Object> row = new HashMap<>();
                row.put(KIND, kind.toString());
                row.put(TRANSFORMATION, transformation.toString());
                row.put(ENTRY_ID, r.getLong("entry_id"));
                row.put(ENTRY_TIME, new Date(r.getLong("wt") / 1000));
                result.put(r.getLong("epoch"), row);
            }
            return result;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
