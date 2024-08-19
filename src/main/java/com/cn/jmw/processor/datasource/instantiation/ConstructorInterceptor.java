package com.cn.jmw.processor.datasource.instantiation;

import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.jar.asm.Type;

import java.util.List;

public class ConstructorInterceptor implements Implementation {
    private final List<String> fieldNames;
    private final List<Class<?>> fieldTypes;

    public ConstructorInterceptor(List<String> fieldNames, List<Class<?>> fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    @Override
    public ByteCodeAppender appender(Target implementationTarget) {
        return new ByteCodeAppender.Simple(
                new StackManipulation.Compound(
                        // 将构造器参数赋值给对应的字段
                        new StackManipulation(){
                            @Override
                            public boolean isValid() {
                                return false;
                            }

                            @Override
                            public Size apply(MethodVisitor methodVisitor, Implementation.Context implementationContext) {
                                for (int i = 0; i < fieldNames.size(); i++) {
                                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // 加载this
                                    methodVisitor.visitVarInsn(Type.getType(fieldTypes.get(i)).getOpcode(Opcodes.ILOAD), i + 1); // 加载参数
                                    methodVisitor.visitFieldInsn(Opcodes.PUTFIELD, implementationTarget.getInstrumentedType().getInternalName(), fieldNames.get(i), Type.getType(fieldTypes.get(i)).getDescriptor());
                                }
                                return new Size(fieldNames.size() * 2, fieldNames.size() * 2);
                            }
                        }
                )
        );
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
        return null;
    }
}