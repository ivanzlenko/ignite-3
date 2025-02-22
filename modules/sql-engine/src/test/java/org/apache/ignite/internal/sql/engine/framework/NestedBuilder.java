/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.framework;

/**
 * This interfaces provides a nested builder with ability to return on the previous layer.
 *
 * <p>For example:</p>
 * <pre>
 *     interface ChildBuilder implements NestedBuilder&lt;ParentBuilder&gt; {
 *         ChildBuilder nestedFoo();
 *     }
 *
 *     interface ParentBuilder {
 *         ParentBuilder foo();
 *         ParentBuilder bar();
 *         ChildBuilder child();
 *     }
 *
 *     Builders.parent()
 *         .foo()
 *         .child() // now we are dealing with the ChildBuilder
 *             .nestedFoo()
 *             .end() // and here we are returning back to the ParentBuilder
 *         .bar()
 *         .build()
 * </pre>
 */
@FunctionalInterface
public interface NestedBuilder<ParentT> {
    /**
     * Notifies the builder's chain of the nested builder that we need to return back to the previous layer.
     *
     * @return An instance of the parent builder.
     */
    ParentT end();
}
