/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.ballerinalang.net.reliable.nativeimpl;

import org.ballerinalang.bre.Context;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BJSON;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.natives.AbstractNativeFunction;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.ReturnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ballerina native function to convert any to json
 */
@BallerinaFunction(packageName = "ballerina.net.reliable",
                   functionName = "anyToJson",
                   args = {
                           @Argument(name = "any value comes after de-serializing a map",
                                     type = TypeKind.ANY)
                   },
                   returnType = {
                           @ReturnType(type = TypeKind.JSON)
                   },
                   isPublic = true)
public class AnyToJson extends AbstractNativeFunction {
    private static final Logger log = LoggerFactory.getLogger(AnyToJson.class);

    public BValue[] execute(Context context) {
        BValue anyValue = getRefArgument(context, 0);
        BJSON jsonValue = (BJSON) anyValue;
        return this.getBValues(jsonValue);
    }
}
