package org.judal.cassandra;

/**
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import static me.prettyprint.hector.api.ddl.ComparatorType.BYTESTYPE;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.hector.api.ddl.ComparatorType;

public class BigDecimalSerializer extends AbstractSerializer<BigDecimal> {

  private static final BigDecimalSerializer INSTANCE = new BigDecimalSerializer();

  public static BigDecimalSerializer get() {
    return INSTANCE;
  }

  @Override
  public BigDecimal fromByteBuffer(ByteBuffer byteBuffer) {
    if (byteBuffer == null) {
      return null;
    }

    int scale = byteBuffer.getInt();

    int length = byteBuffer.remaining();
    byte[] bytes = new byte[length];
    byteBuffer.duplicate().get(bytes);

    return new BigDecimal(new BigInteger(bytes), scale);
  }

  @Override
  public ByteBuffer toByteBuffer(BigDecimal obj) {
    if (obj == null) {
      return null;
    }

    byte[] unscaled = obj.unscaledValue().toByteArray();

    ByteBuffer buff = ByteBuffer.allocate(unscaled.length + 4);

    buff.putInt(obj.scale());
    buff.put(unscaled);

    return buff;
  }

  @Override
  public ComparatorType getComparatorType() {
    return BYTESTYPE;
  }

}