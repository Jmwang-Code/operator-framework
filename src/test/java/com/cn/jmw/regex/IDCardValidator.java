package com.cn.jmw.regex;

import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

public class IDCardValidator {

    private static final Pattern ID_CARD_PATTERN = Pattern.compile(
            "^(?:[1-6][1-7]\\d{4}(?:(?:19|20)\\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\\d|3[01])\\d{3}(?:\\d|X)|\\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\\d|3[01])\\d{3}))$"
    );

    public static boolean isValidIDCard(String idCard) {
        return ID_CARD_PATTERN.matcher(idCard).matches();
    }

    @Test
    public void IDCardValidator() {
        String[] tests = {
                "11010119900307653X", // 18 位，需校验码验证
                "110101900307123",    // 15 位
                "11010119900307653",  // 17 位，错误
                "99999919900307653X", // 非法地址码
                "1234567890123456789" // 19 位
        };
        for (String test : tests) {
            System.out.println(test + ": " + isValidIDCard(test));
        }
    }
}
