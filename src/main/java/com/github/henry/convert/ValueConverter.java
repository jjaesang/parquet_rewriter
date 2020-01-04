package com.github.henry.convert;

import org.apache.parquet.example.data.Group;

public interface ValueConverter {

    Group convertGroup(Group group);

}
