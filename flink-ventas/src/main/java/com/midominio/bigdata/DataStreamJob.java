

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

import java.time.LocalDateTime;
import java.time.DayOfWeek;

public class DataStreamJob {

    public static class EventoEcommerce {
        public String eventType;
        public String productName;
        public String userId;
        public int hora;
        public int dia;
        public int mes;
        public boolean esFinDeSemana;

        @Override
        public String toString() {
            return String.format("[%s] %s | User: %s | Día: %d Mes: %d | FinDeSemana: %b",
                    eventType, productName, userId, dia, mes, esFinDeSemana);
        }
    }

	public static void main(String[] args) throws Exception {
		// 1. Configurar el entorno
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		String kafkaBroker = "IP_DE_TU_KAFKA:9092"; 

		KafkaSource<String> source = KafkaSource.<String>builder()
				.setBootstrapServers(kafkaBroker)
				.setTopics("ventas-en-tiempo-real")
				.setGroupId("mi-grupo-ecommerce")
				.setStartingOffsets(OffsetsInitializer.latest())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> streamCrudo = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<EventoEcommerce> eventos = streamCrudo.map(new MapFunction<String, EventoEcommerce>() {
            @Override
            public EventoEcommerce map(String value) {
                String[] partes = value.split(",");
                EventoEcommerce e = new EventoEcommerce();
                e.eventType = partes[0].trim().toUpperCase();
                e.productName = partes[1].trim();
                e.userId = partes[2].trim();

                // Agregar atributos derivados temporales
                LocalDateTime ahora = LocalDateTime.now();
                e.hora = ahora.getHour();
                e.dia = ahora.getDayOfMonth();
                e.mes = ahora.getMonthValue();
                e.esFinDeSemana = (ahora.getDayOfWeek() == DayOfWeek.SATURDAY || ahora.getDayOfWeek() == DayOfWeek.SUNDAY);
                
                return e;
            }
        });

        // ==========================================
        // REQUISITO 1: Mostrar continuamente todos los eventos
        // ==========================================
        eventos.print("1. TODO");

        // ==========================================
        // 2.2 Filtrar Eventos (Solo PURCHASE y ADD_CART)
        // ==========================================
        DataStream<EventoEcommerce> soloComprasYCarritos = eventos.filter(e -> 
            e.eventType.equals("PURCHASE") || e.eventType.equals("ADD_CART")
        );
        soloComprasYCarritos.print("2.2. FILTRO COMPRAS/CARRITO");

        // ==========================================
        // 2.4 Conteo de eventos generales (Búsquedas, Compras, etc.)
        // ==========================================
        DataStream<Tuple2<String, Integer>> conteoEventos = eventos
            .map(e -> new Tuple2<>(e.eventType, 1))
            .returns(Types.TUPLE(Types.STRING, Types.INT))
            .keyBy(t -> t.f0) // Agrupar por el tipo de evento
            .sum(1);          // Sumar el contador
        conteoEventos.print("2.4. ESTADÍSTICAS GLOBALES");

        // ==========================================
        // 2.5 Agrupamiento por producto (Mayor actividad visualizaciones)
        // ==========================================
        DataStream<Tuple2<String, Integer>> rankingProductos = eventos
            .filter(e -> e.eventType.equals("VIEW_PRODUCT")) // Solo tomar visualizaciones
            .map(e -> new Tuple2<>(e.productName, 1))
            .returns(Types.TUPLE(Types.STRING, Types.INT))
            .keyBy(t -> t.f0) // Agrupar por el nombre del producto
            .sum(1);          // Sumar las visualizaciones
        rankingProductos.print("2.5. RANKING VISUALIZACIONES");

		// Ejecutar la topología
		env.execute("Proyecto Ecommerce Flink");
	}
}
