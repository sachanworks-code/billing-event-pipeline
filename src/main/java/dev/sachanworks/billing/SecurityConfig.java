package dev.sachanworks.billing;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.List;

@Configuration
@EnableWebSecurity
public class SecurityConfig {
    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http, ApiKeyAuthenticationFilter apiKeyAuthenticationFilter) throws Exception {
        http
            .csrf(AbstractHttpConfigurer::disable)
            .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/actuator/health").permitAll()
                .requestMatchers("/error").permitAll()
                .requestMatchers(HttpMethod.GET, "/api/v1/billing-events/**").hasAuthority("ROLE_BILLING")
                .requestMatchers(HttpMethod.POST, "/api/v1/billing-events").hasAuthority("ROLE_BILLING")
                .anyRequest().denyAll())
            .addFilterBefore(apiKeyAuthenticationFilter, UsernamePasswordAuthenticationFilter.class)
            .exceptionHandling(ex -> ex.authenticationEntryPoint((request, response, authException) ->
                response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "API key required")));
        return http.build();
    }

    @Bean
    public ApiKeyAuthenticationFilter apiKeyAuthenticationFilter(@Value("${billing.api-key}") String expectedApiKey) {
        return new ApiKeyAuthenticationFilter(expectedApiKey);
    }

    public static final class ApiKeyAuthenticationFilter extends OncePerRequestFilter {
        private static final String API_KEY_HEADER = "X-API-Key";
        private final String expectedApiKey;

        public ApiKeyAuthenticationFilter(String expectedApiKey) {
            this.expectedApiKey = expectedApiKey == null ? "" : expectedApiKey.trim();
        }

        @Override
        protected boolean shouldNotFilter(HttpServletRequest request) {
            String path = request.getRequestURI();
            return "/actuator/health".equals(path) || "/error".equals(path);
        }

        @Override
        protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
                throws ServletException, IOException {
            String providedApiKey = request.getHeader(API_KEY_HEADER);
            if (expectedApiKey.isBlank() || providedApiKey == null || !expectedApiKey.equals(providedApiKey)) {
                response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Invalid or missing API key");
                return;
            }

            var authentication = new UsernamePasswordAuthenticationToken(
                "billing-service",
                providedApiKey,
                List.of(new SimpleGrantedAuthority("ROLE_BILLING"))
            );
            SecurityContextHolder.getContext().setAuthentication(authentication);
            filterChain.doFilter(request, response);
        }
    }
}
