/*
 * Copyright (C) 2025 Square, Inc.
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
package retrofit2.converter.moshi;

import static retrofit2.converter.moshi.MoshiRequestBodyConverter.MEDIA_TYPE;

import com.squareup.moshi.JsonAdapter;
import java.io.IOException;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okio.BufferedSink;

final class MoshiStreamingRequestBody<T> extends RequestBody {
  private final JsonAdapter<T> adapter;
  private final T value;

  public MoshiStreamingRequestBody(JsonAdapter<T> adapter, T value) {
    this.adapter = adapter;
    this.value = value;
  }

  @Override
  public MediaType contentType() {
    return MEDIA_TYPE;
  }

  @Override
  public void writeTo(BufferedSink sink) throws IOException {
    adapter.toJson(sink, value);
  }
}
