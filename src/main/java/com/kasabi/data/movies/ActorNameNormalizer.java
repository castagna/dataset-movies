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

package com.kasabi.data.movies;

public class ActorNameNormalizer implements StringNormalizer {

	@Override
	public String normalize(String name) {
		String result = name;
		int index = result.indexOf('(');
		if ( index > 0 ) result = result.substring(0, index).trim();
		index = result.indexOf('[');
		if ( index > 0 ) result = result.substring(0, index).trim();
		result = result.replaceAll("\\\\\t", "");
		result = result.replaceAll("and Tony Walker", "Tony Walker");
		result = result.replaceAll("anne Dyson", "Anne Dyson");
		result = result.replaceAll("k.d. lang", "K.D. Lang");
		result = result.replaceAll("the people of Inverness", "");
		result = result.replaceAll("University of Cambridge", "");
		if ( result.length() > 0 ) {
			if ( result.startsWith("Courtesy of ") ) {
				return null;
			} else {
				return result;
			}
		} else {
			return null;
		}
	}

}
