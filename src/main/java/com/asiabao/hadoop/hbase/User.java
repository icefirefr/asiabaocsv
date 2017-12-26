package com.asiabao.hadoop.hbase;

public class User {

	private char name;
	private byte email;
	private int password;
	private float sex;
	private double phone;
	private short height;
	private boolean content;
	private int age;
	
	private byte[] rowkey;

	

	public User(char name, byte email, int password, float sex, double phone,
			short height, boolean content, int age, byte[] rowkey) {
		super();
		this.name = name;
		this.email = email;
		this.password = password;
		this.sex = sex;
		this.phone = phone;
		this.height = height;
		this.content = content;
		this.age = age;
		this.rowkey = rowkey;
	}


	public byte[] getRowkey() {
		return rowkey;
	}


	public void setRowkey(byte[] rowkey) {
		this.rowkey = rowkey;
	}


	public double getPhone() {
		return phone;
	}

	public float getSex() {
		return sex;
	}

	public void setSex(float sex) {
		this.sex = sex;
	}

	public void setPhone(double phone) {
		this.phone = phone;
	}

	public char getName() {
		return name;
	}

	public void setName(char name) {
		this.name = name;
	}

	public byte getEmail() {
		return email;
	}

	public void setEmail(byte email) {
		this.email = email;
	}

	public int getPassword() {
		return password;
	}

	public void setPassword(int password) {
		this.password = password;
	}

	public void setSex(byte sex) {
		this.sex = sex;
	}

	public short getHeight() {
		return height;
	}

	public void setHeight(short height) {
		this.height = height;
	}

	public boolean isContent() {
		return content;
	}

	public void setContent(boolean content) {
		this.content = content;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	@Override
	public String toString() {
		return "User [name=" + name + ", email=" + email + ", password="
				+ password + "]";
	}

}
