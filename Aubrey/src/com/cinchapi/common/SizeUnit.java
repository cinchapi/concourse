/*
 * This project is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 * 
 * This project is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this project. If not, see <http://www.gnu.org/licenses/>.
 */
package com.cinchapi.common;

/**
 * 
 * 
 * @author jnelson
 */
public enum SizeUnit {
	BYTES {
		public double toBytes(long d) {
			return d;
		}
		public double toKiB(long d){
			return d/SI;
		}
		public double toMiB(long d){
			return d/Math.pow(SI, 2);
		}
		public double toGiB(long d){
			return d/Math.pow(SI, 3);
		}
		public double toTiB(long d){
			return d/Math.pow(SI, 4);
		}
		public double convertFrom(long d, SizeUnit u){
			return u.toBytes(d);
		}
	},
	KiB {
		public double toBytes(long d) {
			return d * SI;
		}
		public double toKiB(long d){
			return d;
		}
		public double toMiB(long d){
			return d/SI;
		}
		public double toGiB(long d){
			return d/Math.pow(SI, 2);
		}
		public double toTiB(long d){
			return d/Math.pow(SI, 3);
		}
		public double convertFrom(long d, SizeUnit u){
			return u.toKiB(d);
		}
	},
	MiB {
		public double toBytes(long d) {
			return d * Math.pow(SI, 2);
		}
		public double toKiB(long d){
			return d * SI;
		}
		public double toMiB(long d){
			return d;
		}
		public double toGiB(long d){
			return d/SI;
		}
		public double toTiB(long d){
			return d/Math.pow(SI, 2);
		}
		public double convertFrom(long d, SizeUnit u){
			return u.toMiB(d);
		}
	},
	GiB {
		public double toBytes(long d) {
			return d * Math.pow(SI, 3);
		}
		public double toKiB(long d){
			return d * Math.pow(SI, 2);
		}
		public double toMiB(long d){
			return d * SI;
		}
		public double toGiB(long d){
			return d;
		}
		public double toTiB(long d){
			return d/SI;
		}
		public double convertFrom(long d, SizeUnit u){
			return u.toGiB(d);
		}
	},
	TiB {
		public double toBytes(long d) {
			return d * Math.pow(SI, 4);
		}
		public double toKiB(long d){
			return d * Math.pow(SI, 3);
		}
		public double toMiB(long d){
			return d * Math.pow(SI, 2);
		}
		public double toGiB(long d){
			return d * SI;
		}
		public double toTiB(long d){
			return d;
		}
		public double convertFrom(long d, SizeUnit u){
			return u.toTiB(d);
		}
	};

	static final double SI = 1024L;
	static final double NSI = 1000L;
	
	public double convertFrom(long size, SizeUnit unit) {
		throw new AbstractMethodError();
	}
	
	public double toBytes(long d){
		throw new AbstractMethodError();
	}
	
	public double toKiB(long d){
		throw new AbstractMethodError();
	}
	
	public double toMiB(long d){
		throw new AbstractMethodError();
	}
	
	public double toGiB(long d){
		throw new AbstractMethodError();
	}
	
	public double toTiB(long d){
		throw new AbstractMethodError();
	}

	public static void main(String[] args) {
		System.out.println(SizeUnit.MiB.convertFrom(20000000, SizeUnit.BYTES));
	}

}
