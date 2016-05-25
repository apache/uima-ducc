/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
*/
package org.apache.uima.ducc.common.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.CollectionUtils;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

public class DuccCollectionUtils {

	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <T> DuccListDifference difference(List<T> left, List<T> right) {
		return new DuccListDifference((List<T>)CollectionUtils.subtract(left, right),
					                  (List<T>)CollectionUtils.subtract(right, left));
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <K,T> DuccMapDifference difference(Map<K,T> left, Map<K,T> right) {
		MapDifference<K, T> mapDifference = 
			Maps.difference(left, right);
		//System.out.println(" Map Difference in size:"+mapDifference.entriesDiffering().size());
		return new DuccMapDifference(mapDifference.entriesOnlyOnLeft(),
									 mapDifference.entriesOnlyOnRight(),
									 mapDifference);
	}
	
	public static class DuccMapDifference<K,T> 
	implements Iterable<DuccMapValueDifference<T>> {
		Map<K,T> left;
		Map<K,T> right;
		MapDifference<K, T> mapDifference;
		
		protected DuccMapDifference(Map<K, T> left, Map<K,T> right, MapDifference<K, T> mapDifference) {
			this.left = left;
			this.right = right;
			this.mapDifference = mapDifference;
		}
		public Iterator<DuccMapValueDifference<T>> iterator() {
			return new DuccMapValueDifferenceIterator(getDifferingMap());
		}

		
		public Map<K,T> getLeft() {
			return this.left;
		}
		public Map<K,T> getRight() {
			return this.right;
		}
		public Map<K,T> getCommon() {
			return mapDifference.entriesInCommon();
		}
		public Map<K, MapDifference.ValueDifference<T>> getDifferingMap() {
			return mapDifference.entriesDiffering(); 
		}
		public DuccMapValueDifferenceIterator getDifferingMapValueIterator() {
			return new DuccMapValueDifferenceIterator(getDifferingMap());
		}

		
		public class DuccMapValueDifferenceIterator implements Iterator<DuccMapValueDifference<T>> {
			@SuppressWarnings("unused")
			private Map<K, MapDifference.ValueDifference<T>> differingMap=null;
			private Iterator<Entry<K, MapDifference.ValueDifference<T >>> iterator = null;
			
			protected DuccMapValueDifferenceIterator(Map<K, MapDifference.ValueDifference<T>> differingMap) {
				this.differingMap = differingMap;
				iterator = differingMap.entrySet().iterator();
			}
	        public boolean hasNext() {
	        	return iterator.hasNext();
	        }
	        @SuppressWarnings({ "unchecked", "rawtypes" })
			public DuccMapValueDifference<T> next() {
	        	Entry<K, MapDifference.ValueDifference<T >> dentry =
	        		iterator.next();
				return new DuccMapValueDifference(dentry.getValue().leftValue(),
						dentry.getValue().rightValue());
	        }
	        public void remove() {
	            throw new UnsupportedOperationException();
	        }
		}
	}
	public static class DuccMapValueDifference <T> {
		private T left;
		private T right;
		
		public DuccMapValueDifference(T left, T right) {
			this.left = left;
			this.right = right;
		}
		public T getLeft() {
			return left;
		}
		public T getRight() {
			return right;
		}
	}

	public static class DuccListDifference<T> { 
		List<T> left;
		List<T> right;
		protected DuccListDifference(List<T> left, List<T> right) {
			this.left = left;
			this.right = right;
		}
		
		public List<T> getLeft() {
			return this.left;
		}
		public List<T> getRight() {
			return this.right;
		}
	}
	public static void main(String[] args) {
		try {
			
			List<List<StringHolder>> list1 = new ArrayList<List<StringHolder>>();
			List<List<StringHolder>> list2 = new ArrayList<List<StringHolder>>();
			List<StringHolder> sublist1 = new ArrayList<StringHolder>();
			List<StringHolder> sublist2 = new ArrayList<StringHolder>();
			sublist1.add(new StringHolder("one"));
			sublist1.add(new StringHolder("two"));
			sublist1.add(new StringHolder("three"));
			sublist1.add(new StringHolder("four"));
			sublist1.add(new StringHolder("five"));
			
			sublist2.add(new StringHolder("one"));
			sublist2.add(new StringHolder("two"));
			sublist2.add(new StringHolder("six"));
			sublist2.add(new StringHolder("seven"));
			
			list1.add(sublist1);
			@SuppressWarnings("unchecked")
			DuccListDifference<List<String>> diffList = 
				DuccCollectionUtils.difference(list1, list2);
			System.out.println("Left List Size:"+diffList.getLeft().size() +" Right List Size:"+diffList.getRight().size());
			list2.add(sublist1);
			list1.add(sublist2);
			@SuppressWarnings("unchecked")
			DuccListDifference<List<String>> diffList2 = 
				DuccCollectionUtils.difference(list1, list2);
			System.out.println("Left List Size:"+diffList2.getLeft().size() +" Right List Size:"+diffList2.getRight().size());
			sublist2.add(new StringHolder("eight"));
			list2.add(sublist2);
			
			@SuppressWarnings("unchecked")
			DuccListDifference<List<String>> diffList4 = 
				DuccCollectionUtils.difference(list1, list2);
			System.out.println("*** Left List Size:"+diffList4.getLeft().size() +" Right List Size:"+diffList4.getRight().size());

			
			
			list1.remove(sublist1);
			@SuppressWarnings("unchecked")
			DuccListDifference<List<String>> diffList3 = 
				DuccCollectionUtils.difference(list1, list2);
			System.out.println("Left List Size:"+diffList3.getLeft().size() +" Right List Size:"+diffList3.getRight().size());
		
		} catch( Exception e) {
			e.printStackTrace();
		}
	}
	public static class StringHolder {
		private String value;
		public StringHolder(String value) {
			this.value = value;
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
//			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((value == null) ? 0 : value.hashCode());
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			StringHolder other = (StringHolder) obj;
//			if (!getOuterType().equals(other.getOuterType()))
//				return false;
			if (value == null) {
				if (other.value != null)
					return false;
			} else if (!value.equals(other.value))
				return false;
			return true;
		}
//		private DuccCollectionUtils getOuterType() {
//			return DuccCollectionUtils.this;
//		}
	}
}
