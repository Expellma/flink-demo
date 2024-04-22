package com.flink.lfxu.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class Event implements Serializable {
    private String myKey;
    private Long startTime;
    private Long processTime;
    private Integer count;
}
