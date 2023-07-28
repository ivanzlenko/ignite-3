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

package org.apache.ignite.internal;

import static com.tngtech.archunit.core.domain.properties.CanBeAnnotated.Predicates.annotatedWith;
import static com.tngtech.archunit.lang.conditions.ArchConditions.fullyQualifiedName;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.Dependency;
import com.tngtech.archunit.core.domain.JavaClass.Functions;
import com.tngtech.archunit.core.domain.JavaClass.Predicates;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import org.apache.ignite.lang.LocationProvider.RootLocationProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

/**
 * Tests that tests classes, which uses Mockito framework, extends BaseIgniteAbstractTest. Using BaseIgniteAbstractTest guarantees the
 * Mockito resources will be released after tests finishes.
 */
@SuppressWarnings("JUnitTestCaseWithNoTests")
@AnalyzeClasses(
        packages = "org.apache.ignite",
        locations = RootLocationProvider.class)
public class TestClassHierarchyArchTest {
    private static final String BASE_IGNITE_ABSTRACT_TEST_CLASSNAME = "org.apache.ignite.internal.testframework.BaseIgniteAbstractTest";

    @ArchTest
    public static final ArchRule TEST_CLASS_WITH_MOCKS_EXTENDS_BASE_TEST_CLASS = ArchRuleDefinition
            .classes()
            // are classes with tests
            .that(Predicates.containAnyMethodsThat(annotatedWith(Test.class).or(annotatedWith(ParameterizedTest.class))))
            // uses Mockito framework
            .and(Functions.GET_DIRECT_DEPENDENCIES_FROM_SELF.is(DescribedPredicate.anyElementThat(
                    Dependency.Functions.GET_TARGET_CLASS.is(fullyQualifiedName("org.mockito.Mockito")))
            ))
            .should()
            .beAssignableTo(BASE_IGNITE_ABSTRACT_TEST_CLASSNAME)
            .because("Test class, which uses Mockito framework, must extends BaseIgniteAbstractTest");
}
