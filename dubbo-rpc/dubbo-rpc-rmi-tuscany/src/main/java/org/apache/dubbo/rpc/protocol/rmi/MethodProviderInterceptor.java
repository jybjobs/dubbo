package org.apache.dubbo.rpc.protocol.rmi;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.springframework.remoting.rmi.RmiBasedExporter;

import java.lang.reflect.Method;

public class MethodProviderInterceptor implements MethodInterceptor {

    private  Object targetObject;
    private final RmiBasedExporter rmiExporter;

    public MethodProviderInterceptor(RmiBasedExporter rmiExporter) {
        this.rmiExporter = rmiExporter;
    }

    public MethodProviderInterceptor(Object targetObject, RmiBasedExporter rmiExporter) {
           this.targetObject = targetObject;
           this.rmiExporter = rmiExporter;
    }

    @Override
    public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
        for (Method targetMethod : targetObject.getClass().getMethods()) {
            if(targetMethod.getName().equals(method.getName())){
                return  targetMethod.invoke(targetObject,objects);
            }
        }

        return methodProxy.invoke(o ,objects);
    }
}
