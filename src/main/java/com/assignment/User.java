package com.assignment;

public class User {
	private String userName;
	private Integer empId;
	private String city;
	private Double salary;

	public User(String userName, Integer empId, String city, Double salary) {
		super();
		this.userName = userName;
		this.empId = empId;
		this.city = city;
		this.salary = salary;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public Integer getEmpId() {
		return empId;
	}

	public void setEmpId(Integer empId) {
		this.empId = empId;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public Double getSalary() {
		return salary;
	}

	public void setSalary(Double salary) {
		this.salary = salary;
	}

}
