# Estágio 1: Build (Compila o projeto)
FROM maven:3.8.5-openjdk-17 AS build
WORKDIR /app
COPY . .
# O comando abaixo gera o .jar pulando os testes para ser mais rápido no deploy
RUN mvn clean package -DskipTests

# Estágio 2: Run (Roda o projeto)
FROM openjdk:17.0.1-jdk-slim
WORKDIR /app
# Pega o .jar gerado no estágio anterior
COPY --from=build /app/target/*.jar app.jar
# A porta que o Render usa (obrigatório ser dinâmico ou 8080)
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]