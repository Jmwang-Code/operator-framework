package com.cn.jmw.processor.datasource.instantiation;

import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.jar.asm.Type;

import java.util.Collections;
import java.util.List;

/**
 * 构造函数拦截器
 * <p>
 * 该类通过 ByteBuddy 的字节码操作拦截构造函数调用，将传入的参数赋值给对应的实例字段。
 * 适用于动态生成类并初始化其字段的场景。
 * </p>
 *
 * @author Jmwang
 */
public class ConstructorInterceptor implements Implementation {

    private final List<String> fieldNames;
    private final List<Class<?>> fieldTypes;

    /**
     * 构造函数，初始化字段名称和类型。
     *
     * @param fieldNames 字段名称列表，如果为 null 则使用空列表
     * @param fieldTypes 字段类型列表，如果为 null 则使用空列表
     */
    public ConstructorInterceptor(List<String> fieldNames, List<Class<?>> fieldTypes) {
        this.fieldNames = fieldNames != null ? fieldNames : Collections.emptyList();
        this.fieldTypes = fieldTypes != null ? fieldTypes : Collections.emptyList();
        if (this.fieldNames.size() != this.fieldTypes.size()) {
            throw new IllegalArgumentException("字段名称数量与字段类型数量不匹配");
        }
    }

    /**
     * 创建字节码追加器，用于在构造函数中插入字段赋值逻辑。
     *
     * @param implementationTarget 目标实现信息
     * @return 字节码追加器实例
     */
    @Override
    public ByteCodeAppender appender(Target implementationTarget) {
        return new ByteCodeAppender.Simple(
                new StackManipulation.Compound(
                        new FieldAssignmentManipulation(implementationTarget.getInstrumentedType())
                )
        );
    }

    /**
     * 准备被插装的类型，此处无需修改。
     *
     * @param instrumentedType 被插装的类型
     * @return 未修改的类型
     */
    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
        return instrumentedType;
    }

    /**
     * 字段赋值操作的栈操纵实现
     * <p>
     * 该内部类负责生成字节码，将构造函数参数赋值给对应的实例字段。
     * </p>
     */
    private class FieldAssignmentManipulation implements StackManipulation {

        private final TypeDescription instrumentedType;

        /**
         * 构造函数，接收目标类型描述。
         *
         * @param instrumentedType 目标类型描述
         */
        public FieldAssignmentManipulation(TypeDescription instrumentedType) {
            this.instrumentedType = instrumentedType;
        }

        /**
         * 检查该栈操作是否有效。
         *
         * @return 始终返回 true，表示操作有效
         */
        @Override
        public boolean isValid() {
            return true;
        }

        /**
         * 应用字节码操作，将构造函数参数赋值给字段。
         *
         * @param methodVisitor       方法访问者
         * @param implementationContext 实现上下文
         * @return 栈大小变化信息
         */
        @Override
        public Size apply(MethodVisitor methodVisitor, Implementation.Context implementationContext) {
            for (int i = 0; i < fieldNames.size(); i++) {
                // 加载当前对象 (this)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                // 加载构造函数参数
                Type fieldType = Type.getType(fieldTypes.get(i));
                methodVisitor.visitVarInsn(fieldType.getOpcode(Opcodes.ILOAD), i + 1);
                // 赋值给字段
                methodVisitor.visitFieldInsn(Opcodes.PUTFIELD,
                        instrumentedType.getInternalName(),
                        fieldNames.get(i),
                        fieldType.getDescriptor());
            }
            // 计算栈大小变化：净变化为 0，最大栈深度为 2
            return new Size(0, fieldNames.size() > 0 ? 2 : 0);
        }
    }
}