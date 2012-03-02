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

public class DirectorNameNormalizer implements StringNormalizer {

	@Override
	public String normalize(String name) {
		String result = name;
		result = result.replaceAll("470832", "");
		result = result.replaceAll("99631626", "");
		result = result.replaceAll("\\(uncredited\\)", "");
		result = result.replaceAll("\\(Credited as Producer\\)", "");
		result = result.replaceAll("\\(credited as Producer\\)", "");
		result = result.replaceAll("\\[Credited as Producer\\]", "");
		result = result.replaceAll("\\(Captain Zip\\)", "");
		result = result.replaceAll("Kineto Company of America", "");
		result = result.replaceAll(" et al.", "");
		result = result.replaceAll("rty yiuyuoy", "Rty Yiuyuoy");
		if ( result.length() > 0 ) {
			return result;
		} else {
			return null;
		}
	}

}
