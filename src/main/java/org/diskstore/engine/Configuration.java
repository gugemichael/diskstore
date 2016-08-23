package org.diskstore.engine;

import org.diskstore.engine.option.Option;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Configuration {
    private Map<Option<?>, Object> opsMap = new ConcurrentHashMap<>();

    public <T> Configuration set(Option<T> option, T value) {
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
