import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;

public class GeradorEventosTransporte {

    private static final Random random = new Random();

    private static final String PASTA_SAIDA = "D:\\TCC\\GeradorDeDados\\MassaDeDados";

    private static final String[] STATUS_ROTA = {
            "EM_TRANSITO",
            "PARADO",
            "ATRASADO",
            "FINALIZADO"
    };

    public static void main(String[] args) throws IOException {
        criarPastaSeNaoExistir();

        gerarArquivo(PASTA_SAIDA + "\\eventos_10000.csv", 10000);
        gerarArquivo(PASTA_SAIDA + "\\eventos_100000.csv", 100000);
        gerarArquivo(PASTA_SAIDA + "\\eventos_500000.csv", 500000);

        System.out.println("Arquivos gerados com sucesso em: " + PASTA_SAIDA);
    }

    private static void criarPastaSeNaoExistir() {
        File pasta = new File(PASTA_SAIDA);

        if (!pasta.exists()) {
            boolean criou = pasta.mkdirs();

            if (criou) {
                System.out.println("Pasta criada: " + PASTA_SAIDA);
            } else {
                System.out.println("Não foi possível criar a pasta: " + PASTA_SAIDA);
            }
        }
    }

    private static void gerarArquivo(String caminhoArquivo, int quantidadeEventos) throws IOException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        LocalDateTime dataInicial = LocalDateTime.of(2026, 1, 1, 8, 0, 0);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(caminhoArquivo))) {
            writer.write("id_evento,veiculo_id,latitude,longitude,velocidade,temperatura_carga,status_rota,data_hora");
            writer.newLine();

            for (int i = 1; i <= quantidadeEventos; i++) {
                Long idEvento = Long.valueOf(i);
                Long veiculoId = Long.valueOf(1 + random.nextInt(100));

                double latitude = -27.0966 + gerarVariacao(-0.1000, 0.1000);
                double longitude = -52.6183 + gerarVariacao(-0.1000, 0.1000);
                double velocidade = gerarValor(0.0, 120.0);
                double temperaturaCarga = gerarValor(-5.0, 18.0);

                String statusRota = STATUS_ROTA[random.nextInt(STATUS_ROTA.length)];
                LocalDateTime dataHora = dataInicial.plusSeconds(i);

                String linha = String.format(Locale.US,
                        "%d,%d,%.6f,%.6f,%.2f,%.2f,%s,%s",
                        idEvento,
                        veiculoId,
                        latitude,
                        longitude,
                        velocidade,
                        temperaturaCarga,
                        statusRota,
                        dataHora.format(formatter)
                );

                writer.write(linha);
                writer.newLine();
            }
        }

        System.out.println("Arquivo gerado: " + caminhoArquivo + " com " + quantidadeEventos + " eventos.");
    }

    private static double gerarValor(double minimo, double maximo) {
        return minimo + (maximo - minimo) * random.nextDouble();
    }

    private static double gerarVariacao(double minimo, double maximo) {
        return minimo + (maximo - minimo) * random.nextDouble();
    }
}