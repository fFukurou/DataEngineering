from DadosClass import DadosRepositorios

amazon_rep = DadosRepositorios('amzn')
ling_mais_usadas_amzn = amazon_rep.cria_df_linguagems()

netflix_rep = DadosRepositorios('netflix')
ling_mais_usadas_netflix = netflix_rep.cria_df_linguagems()

spotify_rep = DadosRepositorios('spotify')
ling_mais_usadas_spotify = spotify_rep.cria_df_linguagems()

apple_rep = DadosRepositorios('APPLE')
ling_mais_usadas_apple = apple_rep.cria_df_linguagems()

# Salvando os dados

ling_mais_usadas_amzn.to_csv('dados/linguagens_amzn.csv')
ling_mais_usadas_netflix.to_csv('dados/linguagens_netflix.csv')
ling_mais_usadas_spotify.to_csv('dados/linguagens_spotify.csv')
ling_mais_usadas_apple.to_csv('dados/linguagens_apple.csv')

