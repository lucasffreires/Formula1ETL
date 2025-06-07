from Extracao import Criacaodepastas,Buscardados,Tratamentoarquivo


Criacaodepastas.Criar_pasta_extracao()
Criacaodepastas.Criar_pasta_saida()
Buscardados.Salvar_arquivos(Buscardados().Busca_dados())
Tratamentoarquivo().tratar_arquivos()
Tratamentoarquivo().Converter_para_xlsx()
