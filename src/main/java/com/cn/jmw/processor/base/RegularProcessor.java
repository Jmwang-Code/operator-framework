package com.cn.jmw.processor.base;

import com.cn.jmw.processor.BaseProcessor;


/**
 * 正则表达式处理器
 * <p>
 * 检查输入字符串是否符合特定正则表达式模式。
 * </p>
 *
 * @author Jmwang
 */
public class RegularProcessor extends BaseProcessor<String, String> {

    /**
     * 处理输入字符串，检查是否匹配正则表达式 "^JMWang\\d+$"。
     *
     * @param input 输入字符串
     * @param data  附加数据（未使用）
     * @return 输入字符串如果匹配，否则返回空字符串
     * @throws Exception 如果输入为空
     */
    @Override
    public String process(String input, Object... data) throws Exception {
        if (input == null) {
            throw new IllegalArgumentException("输入参数不能为空");
        }

        String pattern = "^JMWang\\d+$";
        return input.matches(pattern) ? input : "";
    }
}