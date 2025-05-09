/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
 * limitations under the License.
 */

// This script parses the ConcourseService interface from a concourse.thrift
// file and generates an abstract Java class that declares the same methods in
// without the client state arguments (i.e. creds, transaction, environment)

// NOTE: latest version of swift-idl-parser is 0.19.2 but can't be used until
// we standardize on Java 8.
@Grapes([
    @Grab('com.facebook.swift:swift-idl-parser:0.14.2'),
    @Grab('com.google.guava:guava:15.0')
    ]
)
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;

import com.facebook.swift.parser.ThriftIdlParser;
import com.facebook.swift.parser.model.BaseType;
import com.facebook.swift.parser.model.Definition;
import com.facebook.swift.parser.model.Document;
import com.facebook.swift.parser.model.IdentifierType;
import com.facebook.swift.parser.model.ListType;
import com.facebook.swift.parser.model.MapType;
import com.facebook.swift.parser.model.Service;
import com.facebook.swift.parser.model.SetType;
import com.facebook.swift.parser.model.ThriftField;
import com.facebook.swift.parser.model.ThriftMethod;
import com.facebook.swift.parser.model.ThriftType;
import com.facebook.swift.parser.model.VoidType;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;

/**
 * A script that parses the concourse.thrift IDL file and generates the
 * analogous StatefulConcourseService.java file.
 */
public class StatefulConcourseServiceGenerator {

    /**
     * A code stub that populates a mapping from method names to the position
     * of a param that needs to be translated to a TObject before being
     * converted to a ComplexTObject
     */
    private static String TOBJECT_TRANSFORM_CODE = """
    /**
     * A mapping from Thrift method names to a collection of parameter
     * posions that take TObjects. For convenience, a StatefulConcourseService
     * accepts generic objects for those parameters and we must keep track here
     * so it is known what must be translated into a TObject for proper routing
     * in ConcourseServer.
     */
    protected static Multimap<String, Integer> VALUE_TRANSFORM = HashMultimap.create();

    /**
     * A mapping from Thrift method names to a collection of parameter
     * posions that take TCriteria objects. For convenience, a
     * StatefulConcourseService accepts generic objects for those parameters
     * and we must keep track here so it is known what must be translated into
     * a TCriteria for proper routing in ConcourseServer.
     */
    protected static Multimap<String, Integer> CRITERIA_TRANSFORM = HashMultimap.create();

     /**
     * A mapping from Thrift method names to a collection of parameter
     * posions that take TOrder objects. For convenience, a
     * StatefulConcourseService accepts generic objects for those parameters
     * and we must keep track here so it is known what must be translated into
     * a TOrder for proper routing in ConcourseServer.
     */
    protected static Multimap<String, Integer> ORDER_TRANSFORM = HashMultimap.create();

    /**
     * A mapping from Thrift method names to a collection of parameter
     * posions that take TPage objects. For convenience, a
     * StatefulConcourseService accepts generic objects for those parameters
     * and we must keep track here so it is known what must be translated into
     * a TPage for proper routing in ConcourseServer.
     */
    protected static Multimap<String, Integer> PAGE_TRANSFORM = HashMultimap.create();

    /**
     * A collection of Thrift methods that have a return value that contains
     * a TObject. For convenience, a StatefulConcourseService will return
     * generic objects and we must keep track here so it is known what must be
     * translated from a TObject.
     */
    protected static Set<String> RETURN_TRANSFORM = new HashSet<String>();
    static {
    """


    /**
     * Run the program...
     * @param args command line options
     */
    public static void main(String... args) throws IOException {
        String target = args[args.length - 1];
        List<String> signatures = new ArrayList();
        for(int i = 0; i < args.length - 1; ++i) {
            String source = args[i];
            InputSupplier<Reader> input = Files.asCharSource(new File(source), StandardCharsets.UTF_8);
            Document document = ThriftIdlParser.parseThriftIdl(input);
            signatures.addAll(getMethodSignatures(document));
        }
        TOBJECT_TRANSFORM_CODE="""${TOBJECT_TRANSFORM_CODE}
    }
"""        
        String generated = """/**
 * Autogenerated by Codegen Compiler
 *
 * DO NOT EDIT UNLESS YOU KNOW WHAT YOU ARE DOING
 *
 * @generated
 */
package com.cinchapi.concourse.server.plugin;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.cinchapi.concourse.thrift.*;
import com.cinchapi.concourse.lang.Criteria;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.lang.paginate.Page;

/**
 * A modified version of {@link ConcourseService} that maintains client state
 * internally and therefore doesn't require the presentation of state variables
 * (e.g. AccessToken, TransactionToken and environment) as parameters to any
 * methods.
 */
abstract class StatefulConcourseService {

    ${TOBJECT_TRANSFORM_CODE}
"""
        for(String signature : signatures) {
            generated ="""${generated}
    public ${signature} { throw new UnsupportedOperationException(); }
"""
        }
        generated="""${generated}
}
        """
        Files.write(generated, new File(target), StandardCharsets.UTF_8);
    }

    /**
     * A method that traverses the parsed {@link Document} and returns a list
     * of Strings, each of which is a method signature for the generated class.
     *
     * @param  document the parsed {@link Document}
     * @return a list of all the method signatures
     */
    private static List<String> getMethodSignatures(Document document) {
        List<String> signatures = Lists.newArrayList();
        Set<String> bannedMethods = Sets.newHashSet("login", "logout", "stage", "abort", "commit", "invokeManagement");
        Set<String> bannedFields = Sets.newHashSet("creds", "transaction",
                "environment", "token");
        for (Definition definition : document.getDefinitions()) {
            if(definition instanceof Service) {
                Service service = (Service) definition;
                for (ThriftMethod method : service.getMethods()) {
                    String name = method.getName();
                    if(!bannedMethods.contains(name)) {
                        String ret = thriftTypeToJavaType(
                                method.getReturnType(), true);
                        if(ret.contains("TObject") && !ret.contains("ComplexTObject")){
                            ret = ret.replaceAll("TObject", "Object");
                            TOBJECT_TRANSFORM_CODE="""${TOBJECT_TRANSFORM_CODE}
    RETURN_TRANSFORM.add("${name}");
"""
                        }
                        StringBuilder sb = new StringBuilder();
                        int pos = 0 ;
                        for (ThriftField field : method.getArguments()) {
                            if(!bannedFields.contains(field.getName())) {
                                String type = thriftTypeToJavaType(field.getType(),
                                        true);
                                if(type.equals("TObject")){
                                    TOBJECT_TRANSFORM_CODE="""${TOBJECT_TRANSFORM_CODE}
    VALUE_TRANSFORM.put("${name}", ${pos});
"""
                                    type = "Object";
                                }
                                else if(type.contains("TObject") && !type.contains("ComplexTObject")){
                                    type = type.replaceAll("TObject", "Object");
                                    TOBJECT_TRANSFORM_CODE="""${TOBJECT_TRANSFORM_CODE}
    VALUE_TRANSFORM.put("${name}", ${pos});
"""
                                }
                                else if(type.equals("TCriteria")){
                                    TOBJECT_TRANSFORM_CODE="""${TOBJECT_TRANSFORM_CODE}
    CRITERIA_TRANSFORM.put("${name}", ${pos});
"""
                                    type = "Criteria"
                                }
                                else if(type.equals("TOrder")){
                                    TOBJECT_TRANSFORM_CODE="""${TOBJECT_TRANSFORM_CODE}
    ORDER_TRANSFORM.put("${name}", ${pos});
"""
                                    type = "Order"
                                }
                                else if(type.equals("TPage")){
                                    TOBJECT_TRANSFORM_CODE="""${TOBJECT_TRANSFORM_CODE}
    PAGE_TRANSFORM.put("${name}", ${pos});
"""
                                    type = "Page"
                                }
                                sb.append(type);
                                sb.append(" ");
                                sb.append(field.getName());
                                sb.append(", ");
                            }
                            ++pos;
                        }
                        if(sb.length() > 1) {
                            sb.setLength(sb.length() - 2);
                        }
                        String params = sb.toString();
                        String signature = "${ret} ${name}(${params})";
                        signatures.add(signature);
                    }
                }
                break;
            }
        }
        return signatures;
    }

    /**
     * Utility method to convert any {@link ThriftType} to the appropriate Java
     * type that should be added to the method signature.
     *
     * @param   type the methods declared {@link ThriftType}
     * @param   primitive a flag that signals whether an attempt should be made
     *                    to use a primitive Java type
     * @return  the appropriate Java type for the method signature
     */
    private static String thriftTypeToJavaType(ThriftType type,
            boolean primitive) {
        if(type instanceof VoidType) {
            return "void";
        }
        else if(type instanceof BaseType) {
            BaseType base = (BaseType) type;
            String name = base.getType().toString();
            String ret;
            boolean primitiveSupport = true;
            switch (name) {
            case "I64":
                ret = "Long";
                break;
            case "I32":
                ret = "Integer";
                break;
            case "FLOAT":
                ret = "Float";
                break;
            case "DOUBLE":
                ret = "Double";
                break;
            case "STRING":
                ret = "String";
                primitiveSupport = false;
                break;
            case "BOOL":
                ret = "Boolean";
                break;
            case "BINARY":
                ret = "ByteBuffer";
                primitiveSupport = false;
                break;
            default:
                ret = name;
                break;
            }
            return primitive & primitiveSupport ? ret.toLowerCase() : ret;
        }
        else if(type instanceof MapType) {
            MapType map = (MapType) type;
            StringBuilder sb = new StringBuilder();
            sb.append("Map<");
            sb.append(thriftTypeToJavaType(map.getKeyType(), false));
            sb.append(",");
            sb.append(thriftTypeToJavaType(map.getValueType(), false));
            sb.append(">");
            return sb.toString();
        }
        else if(type instanceof SetType) {
            SetType set = (SetType) type;
            StringBuilder sb = new StringBuilder();
            sb.append("Set<");
            sb.append(thriftTypeToJavaType(set.getElementType(), false));
            sb.append(">");
            return sb.toString();
        }
        else if(type instanceof ListType) {
            ListType list = (ListType) type;
            StringBuilder sb = new StringBuilder();
            sb.append("List<");
            sb.append(thriftTypeToJavaType(list.getElementType(), false));
            sb.append(">");
            return sb.toString();
        }
        else if(type instanceof IdentifierType) {
            IdentifierType identifier = (IdentifierType) type;
            String[] parts = identifier.getName().split("\\.");
            return parts[parts.length - 1];
        }
        else {
            return type.toString();
        }
    }
}
