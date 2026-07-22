

package com.midominio.bigdata;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.time.DayOfWeek;

public class DataStreamJob {

    public static class EventoCompra {
        public String user;
        public String event;
        public String product;
        public String category;
        public String city;
        public double price;
        public String timestamp;
        public int hora;
        public int dia;
        public int mes;
        public boolean esFinDeSemana;

        @Override
        public String toString() {
            return String.format("[%s] %s | User: %s | Cat: %s | Ciudad: %s | Precio: %.2f | Día: %d Mes: %d Hora: %d | FinDeSemana: %b",
                    event, product, user, category, city, price, dia, mes, hora, esFinDeSemana);
        }
    }

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setParallelism(2);

		String kafkaBroker = "IP_DE_TU_KAFKA:9092"; 

		KafkaSource<String> source = KafkaSource.<String>builder()
				.setBootstrapServers(kafkaBroker)
				.setTopics("eventos_topic_1")
				.setGroupId("grupo_consumer_1")
				.setStartingOffsets(OffsetsInitializer.latest())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> streamCrudo = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // =============================================
        // 2.3 TRANSFORMACIÓN: Parsear JSON y derivar campos de fecha
        // =============================================
        DataStream<EventoCompra> eventos = streamCrudo.map(new MapFunction<String, EventoCompra>() {
            private transient ObjectMapper mapper;

            @Override
            public EventoCompra map(String value) {
                if (mapper == null) {
                    mapper = new ObjectMapper();
                }

                try {
                    JsonNode json = mapper.readTree(value);

                    EventoCompra e = new EventoCompra();
                    e.user = json.get("user").asText();
                    e.event = json.get("event").asText().toUpperCase();
                    e.product = json.get("product").asText();
                    e.category = json.get("category").asText();
                    e.city = json.get("city").asText();
                    e.price = json.get("price").asDouble();
                    e.timestamp = json.get("timestamp").asText();

                    LocalDateTime fecha = LocalDateTime.parse(e.timestamp);
                    e.hora = fecha.getHour();
                    e.dia = fecha.getDayOfMonth();
                    e.mes = fecha.getMonthValue();
                    e.esFinDeSemana = (fecha.getDayOfWeek() == DayOfWeek.SATURDAY || fecha.getDayOfWeek() == DayOfWeek.SUNDAY);

                    return e;
                } catch (Exception ex) {
                    throw new RuntimeException("Error al parsear JSON: " + value, ex);
                }
            }
        });

        // =============================================
        // 2.1 MOSTRAR TODOS LOS EVENTOS RECIBIDOS
        // =============================================
        eventos.print("2.1 TODOS LOS EVENTOS");

        // =============================================
        // 2.2 FILTRAR: Solo PURCHASE y ADD_CART
        // =============================================
        DataStream<EventoCompra> soloComprasYCarritos = eventos.filter(e -> 
            e.event.equals("PURCHASE") || e.event.equals("ADD_CART")
        );
        soloComprasYCarritos.print("2.2 FILTRO COMPRAS/CARRITO");

        // =============================================
        // 2.4 CONTEO DE EVENTOS (Estadísticas Globales)
        // Número de búsquedas, compras, visualizaciones, agregados al carrito
        // =============================================
        DataStream<Tuple2<String, Integer>> conteoEventos = eventos
            .map(e -> new Tuple2<>(e.event, 1))
            .returns(Types.TUPLE(Types.STRING, Types.INT))
            .keyBy(t -> t.f0) 
            .sum(1);          
        conteoEventos.print("2.4 ESTADÍSTICAS GLOBALES");

        // =============================================
        // 2.5 AGRUPAMIENTO POR PRODUCTO (Mayor actividad)
        // =============================================
        DataStream<Tuple2<String, Integer>> rankingProductos = eventos
            .map(e -> new Tuple2<>(e.product, 1))
            .returns(Types.TUPLE(Types.STRING, Types.INT))
            .keyBy(t -> t.f0) 
            .sum(1);          
        rankingProductos.print("2.5 RANKING PRODUCTOS");

		env.execute("Proyecto Ecommerce Flink");
	}
}
