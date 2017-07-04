package com.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Human implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -81968166121500784L;

	private String name;

	private int age;

	private List<String> apis = new ArrayList<>();

	public String getName() {
		return name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(final int age) {
		this.age = age;
	}

	public List<String> getApis() {
		return apis;
	}

	public void setApis(final List<String> apis) {
		this.apis = apis;
	}

	@Override
	public String toString() {
		return "Human [name=" + name + ", age=" + age + ", apis=" + apis + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + age;
		result = prime * result + ((apis == null) ? 0 : apis.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
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
		Human other = (Human) obj;
		if (age != other.age)
			return false;
		if (apis == null) {
			if (other.apis != null)
				return false;
		} else if (!apis.equals(other.apis))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
}
