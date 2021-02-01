package pers.clare.core.cache;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.cache.annotation.CachePut;
import org.springframework.core.annotation.Order;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;


/**
 * 依權限身分覆寫主站和子站
 */
@Aspect
@Order(Integer.MAX_VALUE)
@Component
public class BeeCachePutAop {
    private ExpressionParser parser = new SpelExpressionParser();

    @Before("@annotation(org.springframework.cache.annotation.CachePut)")
    public void after(JoinPoint joinPoint) {
        CachePut cachePut = ((MethodSignature) joinPoint.getSignature()).getMethod().getDeclaredAnnotation(CachePut.class);
        String[] cacheNames = cachePut.value();
        if (cacheNames == null || cacheNames.length == 0) cacheNames = cachePut.cacheNames();
        if (cacheNames == null || cacheNames.length == 0) return;
        String key = cachePut.key();
        if (StringUtils.isEmpty(key)) {
            key = "";
        } else {
            EvaluationContext context = new StandardEvaluationContext();
            Object[] args = joinPoint.getArgs();
            String[] names = ((MethodSignature) joinPoint.getSignature()).getParameterNames();
            for (int i = 0; i < args.length; i++) {
                context.setVariable(names[i], args[i]);
            }
            key = String.valueOf(parser.parseExpression(key).getValue(context));
        }
        for (String name : cacheNames) {
            BeeCacheContext.onlyClearNotify(name, key);
        }
    }
}
