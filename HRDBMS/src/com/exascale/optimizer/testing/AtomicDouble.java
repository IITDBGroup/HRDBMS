package com.exascale.optimizer.testing;

import java.util.Comparator;  
import java.util.concurrent.atomic.AtomicReference;  
  
public class AtomicDouble extends Number implements Comparable<AtomicDouble> {  
    protected static final long serialVersionUID = -2419445336101038676L;  
    protected AtomicReference<Double> value;  
  
    // Constructors  
    public AtomicDouble() {  
        this(0.0);  
    }  
  
    public AtomicDouble(double initVal) {  
        this(new Double(initVal));  
    }  
      
    public AtomicDouble(Double initVal) {  
        value = new AtomicReference<Double>(initVal);  
    }  
      
    public AtomicDouble(AtomicDouble initVal) {  
        this(initVal.getDoubleValue());  
    }  
      
    public AtomicDouble(String initStrVal) {  
        this(Double.valueOf(initStrVal));  
    }  
      
    // Atomic methods  
    public Double getDoubleValue() {  
        return value.get();  
    }  
      
    public double get() {  
        return getDoubleValue().doubleValue();  
    }  
  
    public void set(double newVal) {  
        value.set(new Double(newVal));  
    }  
      
    public void lazySet(double newVal) {  
        set(newVal);  
    }  
  
    public boolean compareAndSet(double expect, double update) {  
        Double origVal, newVal;  
  
        newVal = new Double(update);  
        while (true) {  
            origVal = getDoubleValue();  
  
            if (Double.compare(origVal.doubleValue(), expect) == 0) {  
                if (value.compareAndSet(origVal, newVal))  
                    return true;   
            } else {  
                return false;  
            }  
        }  
    }  
  
    public boolean weakCompareAndSet(double expect, double update) {  
        return compareAndSet(expect, update);  
    }  
  
    public double getAndSet(double setVal) {  
        while (true) {  
            double origVal = get();  
  
            if (compareAndSet(origVal, setVal)) return origVal;  
        }  
    }  
  
    public double getAndAdd(double delta) {  
        while (true) {  
            double origVal = get();  
            double newVal = origVal + delta;  
            if (compareAndSet(origVal, newVal)) return origVal;  
        }  
    }  
  
    public double addAndGet(double delta) {  
        while (true) {  
            double origVal = get();  
            double newVal = origVal + delta;  
            if (compareAndSet(origVal, newVal)) return newVal;  
        }  
    }  
  
    public double getAndIncrement() {  
        return getAndAdd((double) 1.0);  
    }  
  
    public double getAndDecrement() {  
        return getAndAdd((double) -1.0);  
    }  
  
    public double incrementAndGet() {  
        return addAndGet((double) 1.0);  
    }  
  
    public double decrementAndGet() {  
        return addAndGet((double) -1.0);  
    }  
  
    public double getAndMultiply(double multiple) {  
        while (true) {  
            double origVal = get();  
            double newVal = origVal * multiple;  
            if (compareAndSet(origVal, newVal)) return origVal;  
        }  
    }  
  
    public double multiplyAndGet(double multiple) {  
        while (true) {  
            double origVal = get();  
            double newVal = origVal * multiple;  
            if (compareAndSet(origVal, newVal)) return newVal;  
        }  
    }  
      
    // Methods of the Number class  
    @Override  
    public int intValue() {  
        return getDoubleValue().intValue();  
    }  
      
    @Override  
    public long longValue() {  
        return getDoubleValue().longValue();  
    }  
      
    @Override  
    public float floatValue() {  
        return getDoubleValue().floatValue();  
    }  
      
    @Override  
    public double doubleValue() {  
        return getDoubleValue().doubleValue();  
    }  
      
    @Override  
    public byte byteValue() {  
        return (byte)intValue();  
    }  
      
    @Override  
    public short shortValue() {  
        return (short)intValue();  
    }  
      
    public char charValue() {  
        return (char)intValue();  
    }  
  
    public boolean isNaN() {  
        return getDoubleValue().isNaN();  
    }  
  
    public boolean isInfinite() {  
        return getDoubleValue().isInfinite();  
    }  
  
    // Support methods for hashing and comparing  
    @Override  
    public String toString() {  
        return getDoubleValue().toString();  
    }  
  
    @Override  
    public boolean equals(Object obj) {  
        if (obj == null) return false;  
        if (obj instanceof Double) return (compareTo((Double)obj) == 0);  
        if (obj instanceof AtomicDouble)  
                return (compareTo((AtomicDouble)obj) == 0);  
        return false;  
    }  
      
    @Override  
    public int hashCode() {  
        return getDoubleValue().hashCode();  
    }  
      
    public int compareTo(Double aValue) {  
        return comparator.compare(this, aValue);  
    }  
  
    public int compareTo(AtomicDouble aValue) {  
        return comparator.compare(this, aValue);  
    }  
      
    public static AtomicDoubleComparator comparator =  
                                        new AtomicDoubleComparator();  
    public static class AtomicDoubleComparator  
                    implements Comparator<AtomicDouble> {  
        public int compare(AtomicDouble d1, AtomicDouble d2) {  
            return Double.compare(d1.doubleValue(), d2.doubleValue());  
        }  
          
        public int compare(Double d1, AtomicDouble d2) {  
            return Double.compare(d1.doubleValue(), d2.doubleValue());  
        }  
          
        public int compare(AtomicDouble d1, Double d2) {  
            return Double.compare(d1.doubleValue(), d2.doubleValue());  
        }  
    }  
}