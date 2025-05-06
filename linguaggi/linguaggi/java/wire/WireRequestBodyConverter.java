/*
 * Copyright (C) 2015 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package retrofit2.converter.wire;

import com.squareup.wire.Message;
import com.squareup.wire.ProtoAdapter;
import java.io.IOException;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okio.Buffer;
import retrofit2.Converter;

final class WireRequestBodyConverter<T extends Message<T, ?>> implements Converter<T, RequestBody> {
  static final MediaType MEDIA_TYPE = MediaType.get("application/x-protobuf");

  private final ProtoAdapter<T> adapter;
  private final boolean streaming;

  WireRequestBodyConverter(ProtoAdapter<T> adapter, boolean streaming) {
    this.adapter = adapter;
    this.streaming = streaming;
  }

  @Override
  public RequestBody convert(T value) throws IOException {
    if (streaming) {
      return new WireStreamingRequestBody<>(adapter, value);
    }

    Buffer buffer = new Buffer();
    adapter.encode(buffer, value);
    return RequestBody.create(MEDIA_TYPE, buffer.snapshot());
  }
}
