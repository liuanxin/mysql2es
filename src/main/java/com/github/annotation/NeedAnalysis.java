package com.github.annotation;

import java.lang.annotation.*;

/**
 * Whether the field needs analysis (Chinese word segmentation, pinyin, synonym),
 * useful for generating index mapping,
 * only on the field and the type is String
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface NeedAnalysis {

    /** aggregation ? keyword : text */
    boolean aggregation() default false;
}
