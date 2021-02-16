package com.morris.sparked.models;

/**
 * Just a POJO expressing Coffee available at Seattle Coffee Works.
 * You can find Seattle Coffee Works here : [See Seattle Coffee Works](https://www.seattlecoffeeworks.com/)
 *
 * @author wmm<walimmorris@gmail.com>
 */
public class Coffee {
    private int id;
    private String name;
    private String farmer;
    private String roastLevel;
    private String origin;
    private String link;

    /**
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * set id
     * @param id : the id being set
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @return the coffee name
     */
    public String getName() {
        return name;
    }

    /**
     * set coffee name
     * @param name : the name of the coffee
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return farmer name
     */
    public String getFarmer() {
        return farmer;
    }

    /**
     * set the name of coffee farmer
     * @param farmer : the coffee farmer
     */
    public void setFarmer(String farmer) {
        this.farmer = farmer;
    }

    /**
     * @return coffee roast level
     */
    public String getRoastLevel() {
        return roastLevel;
    }

    /**
     * set coffee roast level
     * @param roastLevel : coffee roast level
     */
    public void setRoastLevel(String roastLevel) {
        this.roastLevel = roastLevel;
    }

    /**
     * @return coffee origin
     */
    public String getOrigin() {
        return origin;
    }

    /**
     * set coffee origin
     * @param origin : coffee origin
     */
    public void setOrigin(String origin) {
        this.origin = origin;
    }

    /**
     * @return link to coffee
     */
    public String getLink() {
        return link;
    }

    /**
     * set link to coffee page
     * @param link : link to coffee page
     */
    public void setLink(String link) {
        this.link = link;
    }
}