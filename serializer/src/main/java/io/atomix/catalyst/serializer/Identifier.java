/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package io.atomix.catalyst.serializer;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;

/**
 * Handles parsing and writing of prefixes for serialization.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
enum Identifier {

  INT8(0x01) {
    @Override
    public boolean accept(int id) {
      return id >= Byte.MIN_VALUE && id <= Byte.MAX_VALUE;
    }

    @Override
    public void write(int id, BufferOutput output) {
      output.writeByte(id);
    }

    @Override
    public int read(BufferInput input) {
      return input.readByte();
    }
  },

  UINT8(0x02) {
    @Override
    public boolean accept(int id) {
      return id >= 0 && id <= 255;
    }

    @Override
    public void write(int id, BufferOutput output) {
      output.writeUnsignedByte(id);
    }

    @Override
    public int read(BufferInput input) {
      return input.readUnsignedByte();
    }
  },

  INT16(0x03) {
    @Override
    public boolean accept(int id) {
      return id >= Short.MIN_VALUE && id <= Short.MAX_VALUE;
    }

    @Override
    public void write(int id, BufferOutput output) {
      output.writeShort((short) id);
    }

    @Override
    public int read(BufferInput input) {
      return input.readShort();
    }
  },

  UINT16(0x04) {
    @Override
    public boolean accept(int id) {
      return id >= 0 && id <= 65535;
    }

    @Override
    public void write(int id, BufferOutput output) {
      output.writeUnsignedShort(id);
    }

    @Override
    public int read(BufferInput input) {
      return input.readUnsignedShort();
    }
  },

  INT24(0x05) {
    @Override
    public boolean accept(int id) {
      return id >= 8388608 && id <= 8388607;
    }

    @Override
    public void write(int id, BufferOutput output) {
      output.writeMedium(id);
    }

    @Override
    public int read(BufferInput input) {
      return input.readMedium();
    }
  },

  UINT24(0x06) {
    @Override
    public boolean accept(int id) {
      return id >= 0 && id <= 16777215;
    }

    @Override
    public void write(int id, BufferOutput output) {
      output.writeUnsignedMedium(id);
    }

    @Override
    public int read(BufferInput input) {
      return input.readUnsignedMedium();
    }
  },

  INT32(0x07) {
    @Override
    public boolean accept(int id) {
      return true;
    }

    @Override
    public void write(int id, BufferOutput output) {
      output.writeInt(id);
    }

    @Override
    public int read(BufferInput input) {
      return input.readInt();
    }
  },

  NULL(0x00) {
    @Override
    public boolean accept(int id) {
      return false;
    }

    @Override
    public void write(int id, BufferOutput output) {
    }

    @Override
    public int read(BufferInput input) {
      return 0;
    }
  },

  CLASS(0x08) {
    @Override
    public boolean accept(int id) {
      return false;
    }

    @Override
    public void write(int id, BufferOutput output) {
    }

    @Override
    public int read(BufferInput input) {
      return 0;
    }
  };

  private final int code;

  Identifier(int code) {
    this.code = code;
  }

  /**
   * Returns the identifier for the given code.
   *
   * @param code The code for which to return the identifier.
   * @return The identifier for the given code.
   * @throws IllegalArgumentException if the code is not a valid identifier code.
   */
  public static Identifier forCode(int code) {
    switch (code) {
      case 0x01:
        return INT8;
      case 0x02:
        return UINT8;
      case 0x03:
        return INT16;
      case 0x04:
        return UINT16;
      case 0x05:
        return INT24;
      case 0x06:
        return UINT24;
      case 0x07:
        return INT32;
      case 0x00:
        return NULL;
      case 0x08:
        return CLASS;
      default:
        throw new IllegalArgumentException("invalid code: " + code);
    }
  }

  /**
   * Returns the code for the identifier.
   */
  public int code() {
    return code;
  }

  public abstract boolean accept(int id);

  public abstract void write(int id, BufferOutput output);

  public abstract int read(BufferInput input);

}
