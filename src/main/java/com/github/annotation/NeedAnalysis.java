package com.github.annotation;

import java.lang.annotation.*;

/**
 * Whether the field needs analysis (Chinese word segmentation, pinyin, synonym),
 * useful for generating index mapping, only on the field and the type is String
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface NeedAnalysis {

    /** text will use: Chinese word segmentation,  keyword with aggregation */
    boolean keyword() default false;

    /** when field need Chinese word segmentation, synonym, set true */
    boolean chinese() default true;

    /** when field need pinyin(String), set true */
    boolean pinyin() default true;

    /** when field need suggest(String), set true */
    boolean suggest() default false;
}
