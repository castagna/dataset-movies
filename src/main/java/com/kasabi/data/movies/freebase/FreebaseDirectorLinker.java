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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;

public class FreebaseDirectorLinker extends FreebaseBaseLinker {

	private static final Logger log = LoggerFactory.getLogger(FreebaseDirectorLinker.class) ;
	
	public FreebaseDirectorLinker(String base) {
		super(base);
	}

	@Override
	public String getURL ( String name ) {
		String result = null;
        try {
			Pair<String,Model> pair = get(name); 
			String url = pair.getLeft();
			if ( url != null ) {
				result = url;
			}
        } catch (Exception e) {
			log.error ( e.getMessage(), e );
		}

        log.debug("getURL({}) --> {}", name, result);
        return result;
	}

}
