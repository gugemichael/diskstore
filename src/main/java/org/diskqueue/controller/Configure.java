package org.diskqueue.controller;

import org.diskqueue.option.Option;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Configure {
    private Map<Option<?>, Object> opsMap = new ConcurrentHashMap<>();

    public <T> Configure set(Option<T> option, T value) {
        opsMap.put(option, value);
        return this;
    }

    /**
     * return specified get name's value if this get is setted. or named default value.
     *
     * @param option named get already defined
     * @return setted value or default value
     */
    @SuppressWarnings("unchecked")
    public <T> T get(Option<T> option) {
        T value;
        if ((value = (T) opsMap.get(option)) != null)
            return value;
        else
            return option.defaultValue();
    }
}
