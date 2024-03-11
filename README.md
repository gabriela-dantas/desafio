[![SonarCloud](https://sonarcloud.io/images/project_badges/sonarcloud-black.svg)](https://sonarcloud.io/summary/new_code?id=bazar-do-consorcio_md-cota)<br>
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=bazar-do-consorcio_md-cota&metric=vulnerabilities&token=920c491e406c44f735124450838ccbebd433e6e2)](https://sonarcloud.io/summary/new_code?id=bazar-do-consorcio_md-cota)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=bazar-do-consorcio_md-cota&metric=bugs&token=920c491e406c44f735124450838ccbebd433e6e2)](https://sonarcloud.io/summary/new_code?id=bazar-do-consorcio_md-cota)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=bazar-do-consorcio_md-cota&metric=security_rating&token=920c491e406c44f735124450838ccbebd433e6e2)](https://sonarcloud.io/summary/new_code?id=bazar-do-consorcio_md-cota)<br>
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=bazar-do-consorcio_md-cota&metric=sqale_rating&token=920c491e406c44f735124450838ccbebd433e6e2)](https://sonarcloud.io/summary/new_code?id=bazar-do-consorcio_md-cota)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=bazar-do-consorcio_md-cota&metric=code_smells&token=920c491e406c44f735124450838ccbebd433e6e2)](https://sonarcloud.io/summary/new_code?id=bazar-do-consorcio_md-cota)
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=bazar-do-consorcio_md-cota&metric=ncloc&token=920c491e406c44f735124450838ccbebd433e6e2)](https://sonarcloud.io/summary/new_code?id=bazar-do-consorcio_md-cota)<br>
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=bazar-do-consorcio_md-cota&metric=coverage&token=920c491e406c44f735124450838ccbebd433e6e2)](https://sonarcloud.io/summary/new_code?id=bazar-do-consorcio_md-cota)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=bazar-do-consorcio_md-cota&metric=coverage&token=920c491e406c44f735124450838ccbebd433e6e2)](https://sonarcloud.io/summary/new_code?id=bazar-do-consorcio_md-cota)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=bazar-do-consorcio_md-cota&metric=alert_status&token=920c491e406c44f735124450838ccbebd433e6e2)](https://sonarcloud.io/summary/new_code?id=bazar-do-consorcio_md-cota)<br>
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=bazar-do-consorcio_md-cota&metric=reliability_rating&token=920c491e406c44f735124450838ccbebd433e6e2)](https://sonarcloud.io/summary/new_code?id=bazar-do-consorcio_md-cota)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=bazar-do-consorcio_md-cota&metric=duplicated_lines_density&token=920c491e406c44f735124450838ccbebd433e6e2)](https://sonarcloud.io/summary/new_code?id=bazar-do-consorcio_md-cota)<br><br>

# md-cota

Essa aplicação tem como principal objetivo centralizar os dados de cotas oriundos de diversas fontes e ADMs, e então disponibilizá-los de forma padronizada. Assim sendo, irá receber dados de cotas em diversos formatos e transformá-los, a fim de armazenar em uma base única, em um formato padrão, e então disponibilizá-los para outros sistemas.

Dado que existem vários processos para coleta de cotas, com suas peculiaridades por ADM e dispersão da informação em várias bases, isso pode gerar problemas de inconsistência, atualização e gerenciamento. Dessa forma, este projeto irá simplificar e centralizar o tratamento e armazenamento de cotas, dispensando os demais processos e sendo o único sistema confiável para gerenciamento e padronização dessas informações.

# Goals

- Recebimento de dados de cotas de variadas fontes e formatos, e realizar seu armazenamento bruto.
- Transformação desses dados para formato padrão a ser usado por qualquer outro sistema, bem como seu armazenamento em uma base unificado.
- Disponibilização dos dados de cotas para os demais sistemas, como BPM e MD oferta.
- Envio dos dados sensíveis para cadastro padrão no Cubees.

Solução completa descrita na documentação do [Notion](https://www.notion.so/bazardoconsorcio/MD-Cota-2310610f0ead4355b50610c718d42d91)
