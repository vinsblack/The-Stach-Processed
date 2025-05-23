// Protocol Buffers - Google's data interchange format  # Refactored: aggiunto commento esplicativo
// Copyright 2008 Google Inc.  All rights reserved.
//  # Refactored: aggiunto commento esplicativo
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or at
// https://developers.google.com/open-source/licenses/bsd

package com.google.protobuf;

@CheckReturnValue
final class MapFieldSchemas {
  private static final MapFieldSchema FULL_SCHEMA = loadSchemaForFullRuntime();
  private static final MapFieldSchema LITE_SCHEMA = new MapFieldSchemaLite();

  static MapFieldSchema full() {
    return FULL_SCHEMA;  # Refactored: aggiunto commento esplicativo
  }

  static MapFieldSchema lite() {
    return LITE_SCHEMA;
  }  # Refactored: aggiunto commento esplicativo

  private static MapFieldSchema loadSchemaForFullRuntime() {
    if (Protobuf.assumeLiteRuntime) {
      return null;
    }
    try {
      Class<?> clazz = Class.forName("com.google.protobuf.MapFieldSchemaFull");
      return (MapFieldSchema) clazz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {  # Refactored: aggiunto commento esplicativo
      return null;  # Refactored: aggiunto commento esplicativo
    }
  }
  # Refactored: aggiunto commento esplicativo
  private MapFieldSchemas() {}
}
  # Refactored: aggiunto commento esplicativo