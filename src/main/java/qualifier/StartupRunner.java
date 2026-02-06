package qualifier;

import java.util.Map;

import org.springframework.boot.CommandLineRunner;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

@Component
public class StartupRunner implements CommandLineRunner {

    private final WebClient webClient = WebClient.create();

    @Override
    public void run(String... args) {

        // 1. Generate webhook
        WebhookResponse response = webClient.post()
            .uri("https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(Map.of(
                "name", "John Doe",
                "regNo", "REG12347",
                "email", "john@example.com"
            ))
            .retrieve()
            .bodyToMono(WebhookResponse.class)
            .block();

        if (response == null) {
            System.out.println("Webhook generation failed");
            return;
        }

        // 2. Submit SQL
        webClient.post()
            .uri(response.getWebhook())
            .header(HttpHeaders.AUTHORIZATION, response.getAccessToken())
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(Map.of(
                "finalQuery", getFinalSqlQuery()
            ))
            .retrieve()
            .bodyToMono(String.class)
            .block();

        System.out.println("SQL query submitted successfully");
    }

    private String getFinalSqlQuery() {
        return """
        SELECT
            d.DEPARTMENT_NAME,
            SUM(p.AMOUNT) AS SALARY,
            CONCAT(e.FIRST_NAME, ' ', e.LAST_NAME) AS EMPLOYEE_NAME,
            TIMESTAMPDIFF(YEAR, e.DOB, CURDATE()) AS AGE
        FROM EMPLOYEE e
        JOIN DEPARTMENT d
            ON e.DEPARTMENT = d.DEPARTMENT_ID
        JOIN PAYMENTS p
            ON e.EMP_ID = p.EMP_ID
        WHERE DAY(p.PAYMENT_TIME) <> 1
        GROUP BY
            d.DEPARTMENT_NAME,
            e.EMP_ID,
            e.FIRST_NAME,
            e.LAST_NAME,
            e.DOB
        HAVING SUM(p.AMOUNT) = (
            SELECT MAX(total_salary)
            FROM (
                SELECT SUM(p2.AMOUNT) AS total_salary
                FROM EMPLOYEE e2
                JOIN PAYMENTS p2
                    ON e2.EMP_ID = p2.EMP_ID
                WHERE e2.DEPARTMENT = e.DEPARTMENT
                  AND DAY(p2.PAYMENT_TIME) <> 1
                GROUP BY e2.EMP_ID
            ) t
        );
        """;
    }
}