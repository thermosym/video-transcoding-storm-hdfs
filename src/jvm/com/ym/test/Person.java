package com.ym.test;

import java.io.Serializable;


public class Person implements Serializable{
	/** 
     *  
     */  
    private static final long serialVersionUID = -2279602642375811203L;  
    private Long id;  
    private String name;  
    private Integer age;  
    /** 
     * @return the id 
     */  
    public Long getId() {  
        return id;  
    }  
    /** 
     * @param id the id to set 
     */  
    public void setId(Long id) {  
        this.id = id;  
    }  
    /** 
     * @return the name 
     */  
    public String getName() {  
        return name;  
    }  
    /** 
     * @param name the name to set 
     */  
    public void setName(String name) {  
        this.name = name;  
    }  
    /** 
     * @return the age 
     */  
    public Integer getAge() {  
        return age;
    }  
    /** 
     * @param age the age to set 
     */  
    public void setAge(Integer age) {  
        this.age = age;  
    }  
      
    public String toString(){  
        return "[Person id="+id+" name="+name+" age="+age+"]";  
    }
}
