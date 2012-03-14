/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kasabi.data.movies.freebase;

import org.openjena.atlas.lib.Pair;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class FreebaseDirectorLinker2 extends FreebaseBaseLinker2 {

	public FreebaseDirectorLinker2(String base) {
		super(base);
	}

	@Override
	public Pair<Resource, Model> link(String name, Resource subject, Property property) {
		Model result = ModelFactory.createDefaultModel();
		Pair<String,Model> pair = get(name);
		if ( pair.getLeft() != null ) {
			result.add(pair.getRight());
			Resource resource = ResourceFactory.createResource(pair.getLeft());
			result.add(subject, property, resource);
			result.add(resource, property, subject);
			return new Pair<Resource,Model>(resource, result);			
		}
		return null;
	}

}
