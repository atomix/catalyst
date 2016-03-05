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
package io.atomix.catalyst.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

/**
 * Utility for reading configuration information from properties.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class PropertiesReader {

  /**
   * Loads a properties reader for the given properties file on the local filesystem.
   *
   * @param propertiesFile The properties file for which to load the reader.
   * @return A new properties reader.
   */
  public static PropertiesReader load(String propertiesFile) {
    return new PropertiesReader(loadProperties(propertiesFile));
  }

  /**
   * Loads a properties reader for the given properties file on the classpath.
   *
   * @param propertiesFile The properties file for which to load the reader.
   * @return A new properties reader.
   */
  public static PropertiesReader loadFromClasspath(String propertiesFile) {
    return new PropertiesReader(loadPropertiesFromClasspath(propertiesFile));
  }

  /**
   * Loads properties from a properties file on the local filesystem.
   */
  private static Properties loadProperties(String propertiesFile) {
    Properties properties = new Properties();
    try (InputStream is = new FileInputStream(propertiesFile)) {
      properties.load(is);
    } catch (IOException e) {
      throw new RuntimeException("failed to load properties", e);
    }
    return properties;
  }

  /**
   * Loads properties from a properties file on the classpath.
   */
  private static Properties loadPropertiesFromClasspath(String propertiesFile) {
    Properties properties = new Properties();
    try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(propertiesFile)) {
      properties.load(is);
    } catch (IOException e) {
      throw new RuntimeException("failed to load properties", e);
    }
    return properties;
  }

  private final Properties properties;

  public PropertiesReader(Properties properties) {
    this.properties = Assert.notNull(properties, "properties");
  }

  /**
   * Returns the underlying properties.
   *
   * @return The underlying properties.
   */
  public Properties properties() {
    return properties;
  }

  /**
   * Reads a collection of properties based on a prefix.
   *
   * @param prefix The prefix for which to read properties.
   * @param factory The factory to call for each property name in the collection.
   * @param <T> The collection value type.
   * @return The collection.
   */
  public <T> Collection<T> getCollection(String prefix, Function<String, T> factory) {
    Collection<T> collection = new ArrayList<>();
    for (String property : properties.stringPropertyNames()) {
      if (property.startsWith(prefix + ".")) {
        collection.add(factory.apply(property));
      }
    }
    return collection;
  }

  /**
   * Returns a map of properties for a given prefix.
   *
   * @param prefix The prefix for which to return a map of property values.
   * @param keyFactory A converter function to convert the map keys.
   * @param valueFactory A converter function to convert the map values.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return The map.
   */
  public <K, V> Map<K, V> getMap(String prefix, Function<String, K> keyFactory, Function<String, V> valueFactory) {
    Map<K, V> map = new HashMap<>();
    for (String property : properties.stringPropertyNames()) {
      if (property.startsWith(prefix + ".")) {
        map.put(keyFactory.apply(property.substring(prefix.length() + 1)),
            valueFactory.apply(properties.getProperty(property)));
      }
    }
    return map;
  }

  /**
   * Reads a class property.
   *
   * @param property The property name.
   * @return The property value.
   * @throws ConfigurationException if the property is not present
   */
  public Class<?> getClass(String property) {
    return getProperty(property, className -> {
      try {
        return Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new ConfigurationException("unknown class: " + className, e);
      }
    });
  }

  /**
   * Reads a class property.
   *
   * @param property The property name.
   * @param defaultValue The default value to return if the property is not present
   * @return The property value.
   */
  public Class<?> getClass(String property, Class<?> defaultValue) {
    return getProperty(property, defaultValue, className -> {
      try {
        return Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new ConfigurationException("unknown class: " + className, e);
      }
    });
  }

  /**
   * Reads a file property.
   *
   * @param property The property name.
   * @return The property value.
   * @throws ConfigurationException if the property is not present
   */
  public File getFile(String property) {
    return getProperty(property, File::new);
  }

  /**
   * Reads a file property.
   *
   * @param property The property name.
   * @param defaultValue The default value to return if the property is not present
   * @return The property value.
   */
  public File getFile(String property, File defaultValue) {
    return getProperty(property, defaultValue, File::new);
  }

  /**
   * Reads an enum property.
   *
   * @param property The property name.
   * @param type The enum type.
   * @param <T> The enum type.
   * @return The property value.
   * @throws ConfigurationException if the property is not present
   */
  public <T extends Enum<T>> T getEnum(String property, Class<T> type) {
    return Enum.valueOf(type, getString(property));
  }

  /**
   * Reads an enum property.
   *
   * @param property The property name.
   * @param type The enum type.
   * @param defaultValue The default value to return if the property is not present
   * @param <T> The enum type.
   * @return The property value.
   */
  public <T extends Enum<T>> T getEnum(String property, Class<T> type, T defaultValue) {
    return Enum.valueOf(type, getString(property, defaultValue.name()));
  }

  /**
   * Reads a string property, returning a default value if the property is not present.
   *
   * @param property The property name.
   * @return The property value.
   * @throws ConfigurationException if the property is not present
   */
  public String getString(String property) {
    return getProperty(property, v -> v);
  }

  /**
   * Reads a string property, returning a default value if the property is not present.
   *
   * @param property The property name.
   * @param defaultValue The default value to return if the property is not present.
   * @return The property value.
   */
  public String getString(String property, String defaultValue) {
    return getProperty(property, defaultValue, v -> v);
  }

  /**
   * Reads a boolean property.
   *
   * @param property The property name.
   * @return The property value.
   * @throws ConfigurationException if the property is not present
   */
  public boolean getBoolean(String property) {
    return getProperty(property, value -> {
      switch (value.trim().toLowerCase()) {
        case "true":
        case "1":
          return true;
        case "false":
        case "0":
          return false;
        default:
          throw new ConfigurationException("invalid property value: " + property + " must be a boolean");
      }
    });
  }

  /**
   * Reads a boolean property.
   *
   * @param property The property name.
   * @param defaultValue The default value to return if the property is not present
   * @return The property value.
   */
  public boolean getBoolean(String property, boolean defaultValue) {
    return getProperty(property, defaultValue, value -> {
      switch (value.trim().toLowerCase()) {
        case "true":
        case "1":
          return true;
        case "false":
        case "0":
          return false;
        default:
          throw new ConfigurationException("invalid property value: " + property + " must be a boolean");
      }
    });
  }

  /**
   * Reads a short property.
   *
   * @param property The property name.
   * @return The property value.
   * @throws ConfigurationException if the property is not present
   */
  public short getShort(String property) {
    return getProperty(property, value -> {
      try {
        return Short.valueOf(value);
      } catch (NumberFormatException e) {
        throw new ConfigurationException("malformed property value: " + property + " must be a short");
      }
    });
  }

  /**
   * Reads a short property.
   *
   * @param property The property name.
   * @param defaultValue The default value to return if the property is not present
   * @return The property value.
   */
  public short getShort(String property, short defaultValue) {
    return getProperty(property, defaultValue, value -> {
      try {
        return Short.valueOf(value);
      } catch (NumberFormatException e) {
        throw new ConfigurationException("malformed property value: " + property + " must be a short");
      }
    });
  }

  /**
   * Reads an integer property.
   *
   * @param property The property name.
   * @return The property value.
   * @throws ConfigurationException if the property is not present
   */
  public int getInteger(String property) {
    return getProperty(property, value -> {
      try {
        return Integer.valueOf(value);
      } catch (NumberFormatException e) {
        throw new ConfigurationException("malformed property value: " + property + " must be an integer");
      }
    });
  }

  /**
   * Reads an integer property.
   *
   * @param property The property name.
   * @param defaultValue The default value to return if the property is not present
   * @return The property value.
   */
  public int getInteger(String property, int defaultValue) {
    return getProperty(property, defaultValue, value -> {
      try {
        return Integer.valueOf(value);
      } catch (NumberFormatException e) {
        throw new ConfigurationException("malformed property value: " + property + " must be an integer");
      }
    });
  }

  /**
   * Reads a long property.
   *
   * @param property The property name.
   * @return The property value.
   * @throws ConfigurationException if the property is not present
   */
  public long getLong(String property) {
    return getProperty(property, value -> {
      try {
        return Long.valueOf(value);
      } catch (NumberFormatException e) {
        throw new ConfigurationException("malformed property value: " + property + " must be a long");
      }
    });
  }

  /**
   * Reads a long property.
   *
   * @param property The property name.
   * @param defaultValue The default value to return if the property is not present
   * @return The property value.
   */
  public long getLong(String property, long defaultValue) {
    return getProperty(property, defaultValue, value -> {
      try {
        return Long.valueOf(value);
      } catch (NumberFormatException e) {
        throw new ConfigurationException("malformed property value: " + property + " must be a long");
      }
    });
  }

  /**
   * Reads a float property.
   *
   * @param property The property name.
   * @return The property value.
   * @throws ConfigurationException if the property is not present
   */
  public float getFloat(String property) {
    return getProperty(property, value -> {
      try {
        return Float.valueOf(value);
      } catch (NumberFormatException e) {
        throw new ConfigurationException("malformed property value: " + property + " must be a float");
      }
    });
  }

  /**
   * Reads a float property.
   *
   * @param property The property name.
   * @param defaultValue The default value to return if the property is not present
   * @return The property value.
   */
  public float getFloat(String property, float defaultValue) {
    return getProperty(property, defaultValue, value -> {
      try {
        return Float.valueOf(value);
      } catch (NumberFormatException e) {
        throw new ConfigurationException("malformed property value: " + property + " must be a float");
      }
    });
  }

  /**
   * Reads a double property.
   *
   * @param property The property name.
   * @return The property value.
   * @throws ConfigurationException if the property is not present
   */
  public double getDouble(String property) {
    return getProperty(property, value -> {
      try {
        return Double.valueOf(value);
      } catch (NumberFormatException e) {
        throw new ConfigurationException("malformed property value: " + property + " must be a double");
      }
    });
  }

  /**
   * Reads a double property.
   *
   * @param property The property name.
   * @param defaultValue The default value to return if the property is not present
   * @return The property value.
   */
  public double getDouble(String property, double defaultValue) {
    return getProperty(property, defaultValue, value -> {
      try {
        return Double.valueOf(value);
      } catch (NumberFormatException e) {
        throw new ConfigurationException("malformed property value: " + property + " must be a double");
      }
    });
  }

  /**
   * Reads a duration property.
   *
   * @param property The property name.
   * @return The property value.
   * @throws ConfigurationException if the property is not present
   */
  public Duration getDuration(String property) {
    return getProperty(property, value -> {
      try {
        return Duration.ofMillis(Long.valueOf(value));
      } catch (NumberFormatException e) {
        throw new ConfigurationException("malformed property value: " + property + " must be a number");
      }
    });
  }

  /**
   * Reads a duration property.
   *
   * @param property The property name.
   * @param defaultValue The default value to return if the property is not present
   * @return The property value.
   */
  public Duration getDuration(String property, Duration defaultValue) {
    return getProperty(property, defaultValue, value -> {
      try {
        return Durations.of(value);
      } catch (NumberFormatException e) {
        throw new ConfigurationException("malformed property value: " + property + " must be a number");
      }
    });
  }

  /**
   * Reads an arbitrary property.
   *
   * @param property The property name.
   * @param transformer A transformer function with which to transform the property value to its appropriate type.
   * @param <T> The property type.
   * @return The property value.
   * @throws ConfigurationException if the property is not present
   */
  private <T> T getProperty(String property, Function<String, T> transformer) {
    Assert.notNull(property, "property");
    String value = properties.getProperty(property);
    if (value == null)
      throw new ConfigurationException("missing property: " + property);
    return transformer.apply(value);
  }

  /**
   * Reads an arbitrary property.
   *
   * @param property The property name.
   * @param defaultValue The default property value.
   * @param transformer A transformer function with which to transform the property value to its appropriate type.
   * @param <T> The property type.
   * @return The property value.
   */
  private <T> T getProperty(String property, T defaultValue, Function<String, T> transformer) {
    Assert.notNull(property, "property");
    String value = properties.getProperty(property);
    if (value == null)
      return defaultValue;
    return transformer.apply(value);
  }

}
