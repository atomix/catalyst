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

import java.util.Properties;

/**
 * Qualified properties.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class QualifiedProperties extends Properties {
  private final String qualifier;

  public QualifiedProperties(String qualifier) {
    super();
    this.qualifier = Assert.notNull(qualifier, "qualifier");
  }

  public QualifiedProperties(Properties defaults, String qualifier) {
    super(defaults);
    if (defaults instanceof QualifiedProperties) {
      this.qualifier = String.format("%s.%s", ((QualifiedProperties) defaults).qualifier, qualifier);
    } else {
      this.qualifier = Assert.notNull(qualifier, "qualifier");
    }
  }

  @Override
  public synchronized Object setProperty(String key, String value) {
    return super.setProperty(String.format("%s.%s", qualifier, key), value);
  }

  @Override
  public String getProperty(String key) {
    return super.getProperty(String.format("%s.%s", qualifier, key));
  }

  @Override
  public String getProperty(String key, String defaultValue) {
    return super.getProperty(String.format("%s.%s", qualifier, key), defaultValue);
  }

}
