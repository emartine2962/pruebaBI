/*
	El siguiente query esta escrito bajo el motor de consulta postgresql v13
	Es importante aclarar que la función filter solo está disponible por el momento para postgresql, pero aún así se puede modificar el query
	para que funcione en otros motores de bases de datos, ya sea definiendo la función o haciendo la parte del script en la línea 46 (execute) con joins o subquerys

	El nombre de la tabla con la información del dataset es data, el esquema es public y la base de datos se llama postgresql

	Se trabajó los resultados en forma de views para permitir la sincronización de la información con cada llamado a la vista meta_data
*/

-- Elimina la tabla meta_table y todos los demás objetos que dependan de esta
drop view meta_table cascade;

-- Crea un análisis parcial de la información 
create or replace view meta_table as (
	select
		fecha_min
		, fecha_uso
		, count(distinct customerid) 																										as valor
		, first_value( count(distinct customerid) ) over (partition by fecha_min order by fecha_uso)										as primer_valor
		, count(distinct customerid)/( first_value( count(distinct customerid) ) over (partition by fecha_min order by fecha_uso) )::float 	as ratio
	from(
		select distinct
			customerid
			, to_char(createdat, 'YYYY-MM')											as fecha_uso
			, min( to_char(createdat, 'YYYY-MM') ) over (partition by customerid )	as fecha_min
		from data
		) f
	group by 1,2
);

-- Script
do $$
declare
  	select_clause text := ''; -- Es el texto con el script que se va a ejecutar en la línea 46
  	dates text[]; -- dates es la lista de los meses de la tabla dinámica
	x text;

-- Llena el array dates con el array_agg de la primera sentencia, luego itera para generar el script en select_clause con las condiciones para cada fecha, por último ejecuta la consulta donde se crea meta_data
begin
	select array_agg(distinct fecha_min) into dates from meta_table;
	foreach x in array dates loop
		select_clause := select_clause || ', max(ratio) filter (where fecha_uso = ''' || x || ''' ) as ' || quote_ident(x);
	end loop;

	execute 'create or replace view meta_data as select fecha_min as id' || select_clause || ' from meta_table group by 1';
end $$;

-- Resultado final
select * from meta_data;