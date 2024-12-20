SELECT * FROM LIVROS WHERE CATEGORIA = 'BIOGRAFIA';


SELECT * FROM LIVROS WHERE CATEGORIA = 'ROMANCE' AND PRECO < 48;


SELECT * FROM LIVROS WHERE CATEGORIA = 'POESIA'
AND (AUTORIA != 'Luís Vaz de Camões' AND AUTORIA != 'Gabriel Pedrosa');


SELECT * FROM LIVROS WHERE CATEGORIA = 'POESIA'
AND NOT (AUTORIA = 'Luís Vaz de Camões' OR AUTORIA = 'Gabriel Pedrosa');


SELECT DISTINCT ID_LIVRO FROM VENDAS
WHERE ID_VENDEDOR = 1
ORDER BY ID_LIVRO
;

DELETE FROM LIVROS WHERE ID_LIVRO = 9;

UPDATE LIVROS SET PRECO = 0.9*PRECO;

-- voltando ao preco original
UPDATE LIVROS SET PRECO = 0.9/PRECO;


