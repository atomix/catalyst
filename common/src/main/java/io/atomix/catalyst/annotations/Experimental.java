/*
 * Copyright 2017 the original author or authors.
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
package io.atomix.catalyst.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that an interface, class or method is experimental.
 * <p>
 * Types annotated with the {@code &#064;Experimental} annotation are considered unstable and
 * are subject to change. Use with caution.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Target({
  ElementType.TYPE,
  ElementType.METHOD
})
@Retention(RetentionPolicy.CLASS)
public @interface Experimental {
}
