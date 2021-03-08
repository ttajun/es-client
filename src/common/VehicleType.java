package com.ttajun.es.common;

public enum VehicleType {
    KTME("ktme","190925_ktme_labeled", "Train No.keyword",
            "Car No.keyword", "W/B.keyword", "TIME.keyword"),
    GLOBIZ("globiz", "190925_globiz_labeled", "열차번호.keyword",
            "차량번호.keyword", "부품식별자.keyword", "생성일시.keyword");

    private String name;
    private String index;
    private String train;
    private String car;
    private String part;
    private String time;

    VehicleType(String name, String index, String train, String car, String part, String time) {
        this.name = name;
        this.index = index;
        this.train = train;
        this.car = car;
        this.part = part;
        this.time = time;
    }

    public String getTime() {
        return time;
    }

    public String getName() {
        return name;
    }

    public String getIndex() {
        return index;
    }

    public String getTrain() {
        return train;
    }

    public String getCar() {
        return car;
    }

    public String getPart() {
        return part;
    }
}
