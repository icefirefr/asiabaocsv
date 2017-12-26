package com.asiabao.hadoop.hbase;

public class UserBuilder {
	
	private char name;
	private byte  email;
	private int  password;
	private float sex;
	private double phone;
	private short height;
	private boolean content;
	private int age;
	private byte[] rowkey;
	
	public UserBuilder buildRowkey(byte[] rowkey){
		this.rowkey = rowkey;
		return this;
	}
	public UserBuilder buildPhone(double phone){
		this.phone = phone;
		return this;
	}
	public UserBuilder buildSex(float sex){
		this.sex = sex;
		return this;
	}
	
	public UserBuilder buildHeight(short height){
		this.height = height;
		return this;
	}
	public UserBuilder buildContent(boolean content){
		this.content = content;
		return this;
	}
	
	public UserBuilder buildName(char name){
		this.name = name;
		return this;
	}
	public UserBuilder buildEmail(byte email){
		this.email = email;
		return this;
	}
	public UserBuilder buildPassword(int password){
		this.password = password;
		return this;
	}
	
	public UserBuilder buildAge(int age){
		this.age = age;
		return this;
	} 
	
	
	public User build() {
		return new User(name, email, password, sex, phone, height, content, age, rowkey);
	}
	
}
