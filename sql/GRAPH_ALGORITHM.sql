-- BFS
CREATE OR REPLACE FUNCTION bfs(bigint, refcursor, refcursor) RETURNS SETOF refcursor AS $$
DECLARE
    source ALIAS FOR $1;
    num_growing_path integer;
    bfs_result ALIAS FOR $2;
    bfs_vertices ALIAS FOR $3;
    current_level integer := 1;
    vertex_exists integer;
BEGIN
    SELECT COUNT(1) INTO vertex_exists FROM vertices WHERE id = source;
    IF vertex_exists < 1 THEN
        RAISE EXCEPTION 'VERTEX NOT EXISTS: %', source;
    END IF;
    -- create bfs edges table
    -- TODO, add index on flag, compare performance
    CREATE TEMPORARY TABLE bfs_edges(edge text, tail bigint, level int, flag int8)
    WITH (appendonly=TRUE,orientation=row)
    ON COMMIT DROP
    DISTRIBUTED BY (tail);

    -- init bfs edges table
    INSERT INTO bfs_edges
    SELECT ''||source||'-->'||edges.dst_id AS edge, edges.dst_id AS tail, 0 AS level, 0 AS flag
    FROM edges WHERE edges.src_id = source;

    -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices(vertex_id bigint, level int)
    WITH (appendonly=TRUE,orientation=row)
    ON COMMIT DROP
    DISTRIBUTED BY (vertex_id);

    -- init discovered_vertices
    INSERT INTO discovered_vertices VALUES (source, -1);
    INSERT INTO discovered_vertices SELECT DISTINCT tail, 0 FROM bfs_edges WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;
    WHILE num_growing_path > 0 LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges SET flag = 1 WHERE flag = 0;
        -- next level
        INSERT INTO bfs_edges
        SELECT t1.tail||'-->'||t1.dst_id,  t1.dst_id, current_level, 0 FROM
        (SELECT bfs_edges.tail, edges.dst_id FROM
        bfs_edges JOIN edges ON bfs_edges.flag = 1 AND bfs_edges.tail = edges.src_id) AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.vertex_id
        WHERE discovered_vertices.vertex_id is NULL;

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        INSERT INTO discovered_vertices SELECT DISTINCT tail, current_level FROM bfs_edges WHERE flag = 0;

        -- increase level
        current_level := current_level + 1;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;
    END LOOP;

    OPEN bfs_result FOR SELECT edge, level FROM bfs_edges ORDER BY level;
    RETURN NEXT bfs_result;

    OPEN bfs_vertices FOR SELECT vertex_id, level FROM discovered_vertices ORDER BY level;
    RETURN NEXT bfs_vertices;

END;
$$ LANGUAGE plpgsql;

/*
BEGIN TRANSACTION;
SELECT bfs(35, 'bfs_e', 'bfs__v');
FETCH ALL IN bfs_e;
FETCH ALL IN bfs__v;
END TRANSACTION;
*/


-- DFS
CREATE OR REPLACE FUNCTION dfs(bigint, refcursor, refcursor) RETURNS SETOF refcursor AS $$
DECLARE
    source ALIAS FOR $1;
    dfs_result ALIAS FOR $2;
    dfs_vertices ALIAS FOR $3;
    vertex_exists integer;
    stack_length integer;
    current_pk_id bigint;
    num_new_discovered_neighbours integer;
BEGIN
    SELECT COUNT(1) INTO vertex_exists FROM vertices WHERE id = source;
    IF vertex_exists < 1 THEN
        RAISE EXCEPTION 'VERTEX NOT EXISTS: %', source;
    END IF;
    CREATE TEMPORARY TABLE dfs_paths(path text) WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP;

    -- auto increase pk to simulate FILO action of stack
    CREATE TEMPORARY TABLE dfs_growing_paths(
        pk_id bigint PRIMARY KEY,
        path text,
        vertex_id bigint,
        flag int8
    ) ON COMMIT DROP DISTRIBUTED BY (pk_id);
    CREATE TEMPORARY SEQUENCE pkid_auto_incr START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE
    CACHE 1024 OWNED BY dfs_growing_paths.pk_id;
    -- ALTER TABLE dfs_growing_paths ALTER COLUMN pk_id SET DEFAULT NEXTVAL('pkid_auto_incr');

    -- init dfs_growing_paths
    INSERT INTO dfs_growing_paths
    SELECT NEXTVAL('pkid_auto_incr'), ''||source||'-->'||edges.dst_id, edges.dst_id, 0
    FROM edges WHERE src_id = source;

    -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices(vertex_id bigint)
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (vertex_id);
    -- init discovered_vertices
    INSERT INTO discovered_vertices VALUES (source);
    INSERT INTO discovered_vertices SELECT DISTINCT vertex_id FROM dfs_growing_paths WHERE flag = 0;

    UPDATE dfs_growing_paths SET flag = 1 WHERE flag = 0;

    SELECT COUNT(1) INTO stack_length FROM dfs_growing_paths WHERE flag = 1;

    WHILE stack_length > 0 LOOP
            -- pop the last in vertex
            SELECT MAX(pk_id) INTO current_pk_id FROM dfs_growing_paths WHERE flag = 1;
            -- deep search
            INSERT INTO dfs_growing_paths
            SELECT NEXTVAL('pkid_auto_incr'), t1.path||'-->'||t1.vertex_id, t1.vertex_id, 0 FROM
            (SELECT dfs_growing_paths.path, edges.dst_id AS vertex_id
            FROM dfs_growing_paths JOIN edges
            ON dfs_growing_paths.pk_id = current_pk_id AND dfs_growing_paths.vertex_id = edges.src_id) AS t1
            LEFT JOIN discovered_vertices ON t1.vertex_id = discovered_vertices.vertex_id
            WHERE discovered_vertices.vertex_id IS NULL;
            -- vertex whose neighbours have been discovered
            UPDATE dfs_growing_paths SET flag = 2 WHERE pk_id = current_pk_id;
            -- whether leaf has reached
            SELECT COUNT(1) INTO num_new_discovered_neighbours
            FROM dfs_growing_paths WHERE flag = 0;
            IF num_new_discovered_neighbours > 0 THEN
                INSERT INTO discovered_vertices SELECT DISTINCT vertex_id FROM dfs_growing_paths WHERE flag = 0;
                UPDATE dfs_growing_paths SET flag = 1 WHERE flag = 0;
            ELSE
                INSERT INTO dfs_paths SELECT path FROM dfs_growing_paths WHERE pk_id = current_pk_id;
            END IF;

            SELECT COUNT(1) INTO stack_length FROM dfs_growing_paths WHERE flag = 1;

        END LOOP;

    OPEN dfs_result FOR SELECT path FROM dfs_paths;
    RETURN NEXT dfs_result;

    OPEN dfs_vertices FOR SELECT vertex_id FROM discovered_vertices;
    RETURN NEXT dfs_vertices;

END;
$$ LANGUAGE plpgsql;


/*
BEGIN TRANSACTION;
SELECT dfs(35, 'dfs_e', 'dfs_v');
FETCH ALL IN dfs_e;
FETCH ALL IN dfs_v;
END TRANSACTION;
*/


-- longest path
CREATE OR REPLACE FUNCTION get_longest_path(refcursor) RETURNS refcursor AS $$
DECLARE
	longest_path ALIAS FOR $1;
	num_growing_path integer;
BEGIN
	CREATE TEMPORARY TABLE paths(path text, tail bigint, flag boolean) ON COMMIT DROP DISTRIBUTED BY (tail);
	INSERT INTO paths
	SELECT ''||zero_in_degree.src AS path,  edges.dst_id as tail , TRUE AS falg FROM
	(SELECT t1.src_id AS src FROM
	(SELECT DISTINCT src_id FROM edges) AS t1 LEFT JOIN
	(SELECT DISTINCT dst_id FROM edges) AS t2
	ON t1.src_id = t2.dst_id WHERE t2.dst_id IS NULL) AS zero_in_degree
	JOIN edges ON zero_in_degree.src = edges.src_id;

	SELECT COUNT(1) INTO num_growing_path FROM paths WHERE tail IS NOT NULL AND flag;

	WHILE num_growing_path > 0 LOOP
		-- if path keep growing, then the last iteration can not be longest path
		DELETE FROM paths WHERE NOT flag;
		UPDATE paths SET flag=FALSE;
		UPDATE paths SET path=path||'-->'||tail WHERE tail IS NOT NULL;
		INSERT INTO paths
		SELECT paths.path, edges.dst_id AS tail, TRUE AS flag FROM
		paths JOIN edges ON paths.tail IS NOT NULL AND paths.tail = edges.src_id;
		SELECT COUNT(1) INTO num_growing_path FROM paths WHERE tail IS NOT NULL AND flag;
	END LOOP;

	OPEN longest_path FOR SELECT path FROM paths WHERE NOT flag AND tail IS NOT NULL;

	RETURN longest_path;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN TRANSACTION;
SELECT get_longest_path('longest_path');
FETCH ALL IN longest_path;
END TRANSACTION;
*/


-- longest weighted path
CREATE OR REPLACE FUNCTION get_longest_weighted_path(refcursor) RETURNS refcursor AS $$
DECLARE
	longest_weighted_path ALIAS FOR $1;
	num_growing_path integer;
BEGIN
	CREATE TEMPORARY TABLE paths(path text, tail bigint, weight numeric, flag boolean)
	WITH (appendonly=TRUE,orientation=row)
	ON COMMIT DROP
	DISTRIBUTED BY (tail);
	INSERT INTO paths
	SELECT ''||zero_in_degree.src||'-->'||edges.dst_id AS path, edges.dst_id as tail, edges.weight, TRUE FROM
	(SELECT t1.src_id AS src FROM
	(SELECT DISTINCT src_id FROM edges) AS t1 LEFT JOIN
	(SELECT DISTINCT dst_id FROM edges) AS t2
	ON t1.src_id = t2.dst_id WHERE t2.dst_id IS NULL) AS zero_in_degree
	JOIN edges ON zero_in_degree.src = edges.src_id;

	SELECT COUNT(1) INTO num_growing_path FROM paths WHERE tail IS NOT NULL AND flag;

	WHILE num_growing_path > 0 LOOP
		UPDATE paths SET flag=FALSE WHERE flag;
		INSERT INTO paths
		SELECT paths.path||'-->'||edges.dst_id AS path, edges.dst_id AS tail,
		paths.weight+edges.weight AS weight, TRUE FROM
		paths JOIN edges ON paths.tail IS NOT NULL AND paths.tail = edges.src_id;
		SELECT COUNT(1) INTO num_growing_path FROM paths WHERE tail IS NOT NULL AND flag;
	END LOOP;

	OPEN longest_weighted_path FOR SELECT path FROM paths WHERE weight = (SELECT max(weight) FROM paths);

	RETURN longest_weighted_path;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN TRANSACTION;
SELECT get_longest_weighted_path('longest_weighted_path');
FETCH ALL IN longest_weighted_path;
END TRANSACTION;
*/


-- shortest path using BFS
CREATE OR REPLACE FUNCTION shortest_path(bigint, bigint, refcursor) RETURNS refcursor AS $$
DECLARE
    source ALIAS FOR $1;
    target ALIAS FOR $2;
    path_found ALIAS FOR $3;
    num_growing_path integer;
    vertex_exists integer;
    reached integer;
BEGIN
    SELECT COUNT(1) INTO vertex_exists FROM vertices WHERE id = source;
    IF vertex_exists < 1 THEN
        RAISE EXCEPTION 'VERTEX NOT EXISTS: %', source;
    END IF;
    SELECT COUNT(1) INTO vertex_exists FROM vertices WHERE id = target;
    IF vertex_exists < 1 THEN
        RAISE EXCEPTION 'VERTEX NOT EXISTS: %', target;
    END IF;
    -- create bfs edges table
    -- TODO, add index on flag, compare performance
    CREATE TEMPORARY TABLE bfs_paths(path text, tail bigint, flag int8)
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (tail);

    -- init bfs edges table
    INSERT INTO bfs_paths
    SELECT ''||source||'-->'||edges.dst_id, edges.dst_id AS tail, 0 AS flag
    FROM edges WHERE edges.src_id = source;

    -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices(vertex_id bigint)
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (vertex_id);

    -- init discovered_vertices
    INSERT INTO discovered_vertices VALUES (source);
    INSERT INTO discovered_vertices SELECT DISTINCT tail FROM bfs_paths WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_paths WHERE flag = 0;
    WHILE num_growing_path > 0 LOOP
        -- check if target occurred in new discovered neighbours
        SELECT COUNT(1) INTO reached FROM bfs_paths WHERE flag = 0 AND tail = target;
        IF reached > 0 THEN
            OPEN path_found FOR SELECT path FROM bfs_paths WHERE flag = 0 AND tail = target;
            RETURN path_found;
        END IF;
        -- vertices have been added in discovered vertices
        UPDATE bfs_paths SET flag = 1 WHERE flag = 0;
        -- next level
        INSERT INTO bfs_paths
        SELECT t1.path||'-->'||t1.dst_id,  t1.dst_id, 0 FROM
        (SELECT bfs_paths.path, edges.dst_id FROM
        bfs_paths JOIN edges ON bfs_paths.flag = 1 AND bfs_paths.tail = edges.src_id) AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.vertex_id
        WHERE discovered_vertices.vertex_id is NULL;
        -- vertices whose neighbours have been discovered
        UPDATE bfs_paths SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        INSERT INTO discovered_vertices SELECT DISTINCT tail FROM bfs_paths WHERE flag = 0;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_paths WHERE flag = 0;
    END LOOP;

    RAISE EXCEPTION 'NO PATH BETWEEN % AND %', source, target;

END;
$$ LANGUAGE plpgsql;


-- shortest weighted path using BFS
CREATE OR REPLACE FUNCTION shortest_weighted_path(bigint, bigint, refcursor) RETURNS refcursor AS $$
DECLARE
    source ALIAS FOR $1;
    target ALIAS FOR $2;
    path_found ALIAS FOR $3;
    num_growing_path integer;
    vertex_exists integer;
    min_weight numeric;
BEGIN
    SELECT COUNT(1) INTO vertex_exists FROM vertices WHERE id = source;
    IF vertex_exists < 1 THEN
        RAISE EXCEPTION 'VERTEX NOT EXISTS: %', source;
    END IF;
    SELECT COUNT(1) INTO vertex_exists FROM vertices WHERE id = target;
    IF vertex_exists < 1 THEN
        RAISE EXCEPTION 'VERTEX NOT EXISTS: %', target;
    END IF;
    -- create bfs edges table
    -- TODO, add index on flag, compare performance
    CREATE TEMPORARY TABLE bfs_paths(path text, tail bigint, weight numeric, flag int8)
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (tail);

    -- init bfs edges table
    INSERT INTO bfs_paths
    SELECT ''||source||'-->'||edges.dst_id, edges.dst_id AS tail, edges.weight, 0 AS flag
    FROM edges WHERE edges.src_id = source;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_paths WHERE flag = 0;
    WHILE num_growing_path > 0 LOOP
        -- end path if target reached
        UPDATE bfs_paths SET flag = 3 WHERE flag = 0 AND tail = target;
        -- vertices have been added in discovered vertices
        UPDATE bfs_paths SET flag = 1 WHERE flag = 0;
        -- continue if vertex not occurred in current path
        INSERT INTO bfs_paths
        SELECT t1.path||'-->'||t1.dst_id, t1.dst_id, t1.weight, 0 FROM
        (SELECT bfs_paths.path, bfs_paths.weight + edges.weight AS weight, edges.dst_id FROM
        bfs_paths JOIN edges ON bfs_paths.flag = 1 AND bfs_paths.tail = edges.src_id) AS t1
        WHERE POSITION(''||t1.dst_id IN t1.path) = 0;
        -- vertices whose neighbours have been discovered
        UPDATE bfs_paths SET flag = 2 WHERE flag = 1;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_paths WHERE flag = 0;
    END LOOP;

    SELECT MIN(weight) INTO min_weight FROM bfs_paths WHERE flag = 3;
    IF min_weight IS NULL THEN
        RAISE EXCEPTION 'NO PATH BETWEEN % AND %', source, target;
    ELSE
        OPEN path_found FOR SELECT path, weight FROM bfs_paths WHERE flag = 3 AND weight <= min_weight;
        RETURN path_found;
    END IF;

END;
$$ LANGUAGE plpgsql;


-- BFS for strongly connected component
CREATE OR REPLACE FUNCTION scc_bfs(bigint) RETURNS VOID AS $$
DECLARE
    source ALIAS FOR $1;
    num_growing_path integer;
    vertex_exists integer;
BEGIN
    SELECT COUNT(1) INTO vertex_exists FROM vertices WHERE id = source;
    IF vertex_exists < 1 THEN
        RAISE EXCEPTION 'VERTEX NOT EXISTS: %', source;
    END IF;
    -- create bfs edges table
    -- TODO, add index on flag, compare performance
    CREATE TEMPORARY TABLE bfs_edges(tail bigint, flag int8)
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (tail);

    -- init bfs edges table
    INSERT INTO bfs_edges
    SELECT edges.dst_id AS tail, 0 AS flag
    FROM edges WHERE edges.src_id = source;

    -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices(vertex_id bigint)
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (vertex_id);

    -- init discovered_vertices
    INSERT INTO discovered_vertices VALUES (source);
    INSERT INTO discovered_vertices SELECT DISTINCT tail FROM bfs_edges WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;
    WHILE num_growing_path > 0 LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges SET flag = 1 WHERE flag = 0;
        -- next level
        INSERT INTO bfs_edges
        SELECT t1.dst_id, 0 FROM
        (SELECT edges.dst_id FROM
        bfs_edges JOIN edges ON bfs_edges.flag = 1 AND bfs_edges.tail = edges.src_id) AS t1
        LEFT JOIN discovered_vertices ON t1.dst_id = discovered_vertices.vertex_id
        WHERE discovered_vertices.vertex_id is NULL;

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        INSERT INTO discovered_vertices SELECT DISTINCT tail FROM bfs_edges WHERE flag = 0;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges WHERE flag = 0;
    END LOOP;

END;
$$ LANGUAGE plpgsql;


-- reversed BFS for strongly connected component
CREATE OR REPLACE FUNCTION scc_bfs_reversed(bigint) RETURNS VOID AS $$
DECLARE
    source ALIAS FOR $1;
    num_growing_path integer;
    vertex_exists integer;
BEGIN
    SELECT COUNT(1) INTO vertex_exists FROM vertices WHERE id = source;
    IF vertex_exists < 1 THEN
        RAISE EXCEPTION 'VERTEX NOT EXISTS: %', source;
    END IF;
    -- create bfs edges table
    -- TODO, add index on flag, compare performance
    CREATE TEMPORARY TABLE bfs_edges_reversed(tail bigint, flag int8)
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (tail);

    -- init bfs edges table
    INSERT INTO bfs_edges_reversed
    SELECT edges.src_id AS tail, 0 AS flag
    FROM edges WHERE edges.dst_id = source;

    -- create discovered vertices table
    CREATE TEMPORARY TABLE discovered_vertices_reversed(vertex_id bigint)
    WITH (appendonly=TRUE,orientation=row) ON COMMIT DROP DISTRIBUTED BY (vertex_id);

    -- init discovered_vertices_reversed
    INSERT INTO discovered_vertices_reversed VALUES (source);
    INSERT INTO discovered_vertices_reversed SELECT DISTINCT tail FROM bfs_edges_reversed WHERE flag = 0;

    SELECT COUNT(1) INTO num_growing_path FROM bfs_edges_reversed WHERE flag = 0;
    WHILE num_growing_path > 0 LOOP
        -- vertices have been added in discovered vertices
        UPDATE bfs_edges_reversed SET flag = 1 WHERE flag = 0;
        -- next level
        INSERT INTO bfs_edges_reversed
        SELECT t1.src_id, 0 FROM
        (SELECT edges.src_id FROM
        bfs_edges_reversed JOIN edges ON bfs_edges_reversed.flag = 1 AND bfs_edges_reversed.tail = edges.dst_id) AS t1
        LEFT JOIN discovered_vertices_reversed ON t1.src_id = discovered_vertices_reversed.vertex_id
        WHERE discovered_vertices_reversed.vertex_id is NULL;

        -- vertices whose neighbours have been discovered
        UPDATE bfs_edges_reversed SET flag = 2 WHERE flag = 1;
        -- add into discovered vertices
        INSERT INTO discovered_vertices_reversed SELECT DISTINCT tail FROM bfs_edges_reversed WHERE flag = 0;

        SELECT COUNT(1) INTO num_growing_path FROM bfs_edges_reversed WHERE flag = 0;
    END LOOP;

END;
$$ LANGUAGE plpgsql;


-- strongly_connected_component
CREATE OR REPLACE FUNCTION strongly_connected_component(bigint, refcursor) RETURNS refcursor AS $$
DECLARE
	source ALIAS FOR $1;
	scc ALIAS FOR $2;
	vertex_exists integer;
BEGIN
	-- check if vertex exists
	SELECT COUNT(1) INTO vertex_exists FROM vertices WHERE id = source;
    IF vertex_exists < 1 THEN
        RAISE EXCEPTION 'VERTEX NOT EXISTS: %', source;
    END IF;

	PERFORM scc_bfs(source);
	PERFORM scc_bfs_reversed(source);

	OPEN scc FOR SELECT discovered_vertices.vertex_id
	FROM discovered_vertices JOIN discovered_vertices_reversed
	ON discovered_vertices.vertex_id = discovered_vertices_reversed.vertex_id;

	RETURN scc;

END;
$$ LANGUAGE plpgsql;

/*
BEGIN TRANSACTION;
SELECT strongly_connected_component(35, 'scc');
FETCH ALL IN scc;
COMMIT;
*/


-- topological sorting
CREATE OR REPLACE FUNCTION topological_sorting(refcursor) RETURNS refcursor AS $$
DECLARE
	topo_sorted_vertices ALIAS FOR $1;
	zero_in_degree_vertex_count integer;
	non_zero_in_degree_vertex_count integer;
BEGIN
	-- create topological sorting result table
	CREATE TEMPORARY TABLE topo_sorted_t(vertex_id bigint)
	WITH (appendonly=true,orientation=row) ON COMMIT DROP DISTRIBUTED BY(vertex_id);
	-- insert isolate vertices
	INSERT INTO topo_sorted_t
	SELECT vertices.id  AS vertex_id FROM
	vertices LEFT JOIN
	(SELECT DISTINCT src_id FROM edges UNION SELECT DISTINCT dst_id FROM edges) AS t1
	ON vertices.id=t1.src_id WHERE t1.src_id IS NULL;

	-- create in degree tmp table
	CREATE TEMPORARY TABLE in_degree_t(vertex_id bigint, in_degree int) ON COMMIT DROP DISTRIBUTED BY(vertex_id);
	-- init in degree table
	INSERT INTO in_degree_t SELECT dst_id AS vertex_id, COUNT(1) AS in_degree FROM edges GROUP BY dst_id;
	-- create zero in degree tmp table
	CREATE TEMPORARY TABLE zero_in_degree_t(vertex_id bigint) ON COMMIT DROP DISTRIBUTED BY(vertex_id);
	-- init zero degree table
	INSERT INTO zero_in_degree_t
	SELECT t1.src_id AS vertex_id FROM
	(SELECT DISTINCT src_id FROM edges) AS t1 LEFT JOIN
	(SELECT DISTINCT dst_id FROM edges) AS t2
	ON t1.src_id = t2.dst_id WHERE t2.dst_id IS NULL;

	SELECT COUNT(1) INTO zero_in_degree_vertex_count FROM zero_in_degree_t;
	WHILE zero_in_degree_vertex_count > 0 LOOP
		-- insert zero in degree vertex
		INSERT INTO topo_sorted_t SELECT vertex_id FROM zero_in_degree_t;
		-- update in degree info
		UPDATE in_degree_t SET in_degree=in_degree_t.in_degree-t1.in_degree FROM
		(SELECT edges.dst_id AS vertex_id, COUNT(1) AS in_degree FROM zero_in_degree_t JOIN
		edges ON zero_in_degree_t.vertex_id = edges.src_id GROUP BY edges.dst_id) AS t1
		WHERE in_degree_t.vertex_id = t1.vertex_id;
		-- extract & delete zero in degree vertices
		TRUNCATE zero_in_degree_t;
		INSERT INTO zero_in_degree_t SELECT vertex_id FROM in_degree_t WHERE in_degree = 0;
		DELETE FROM in_degree_t WHERE in_degree = 0;
		SELECT COUNT(1) INTO zero_in_degree_vertex_count FROM zero_in_degree_t;
	END LOOP;

	SELECT COUNT(1) INTO non_zero_in_degree_vertex_count FROM in_degree_t;
	-- cycled graph
	IF non_zero_in_degree_vertex_count > 0 THEN
		RAISE EXCEPTION 'NO TOPOLOGICAL SORTING FOR CYCLED GRAPH';
		-- OPEN topo_sorted_vertices FOR SELECT 1 AS vertex_id WHERE 1 <> 1;
	ELSE
		OPEN topo_sorted_vertices FOR SELECT vertex_id FROM topo_sorted_t;
	END IF;

	RETURN topo_sorted_vertices;
END;
$$ LANGUAGE plpgsql;


/*
BEGIN TRANSACTION;
SELECT topological_sorting('topo_sort');
FETCH ALL IN topo_sort;
END TRANSACTION;
*/

-- get in degree of a specific vertex

CREATE OR REPLACE FUNCTION get_vertex_in_degree(bigint) RETURNS integer AS $$
DECLARE
	in_degree integer;
BEGIN
	SELECT COUNT(1) INTO in_degree FROM edges WHERE dst_id = $1;
	RETURN in_degree;
END;
$$ LANGUAGE plpgsql STABLE;


-- get in degree of all vertices

CREATE OR REPLACE FUNCTION get_in_degree_stats(refcursor, boolean DEFAULT TRUE) RETURNS refcursor AS $$
DECLARE
	in_degree_stats ALIAS FOR $1;
	ascending ALIAS FOR $2;
BEGIN
	IF ascending THEN
	    OPEN in_degree_stats FOR SELECT dst_id AS vertex_id, COUNT(1) AS in_degree FROM edges GROUP BY dst_id ORDER BY in_degree ASC;
	ELSE
	    OPEN in_degree_stats FOR SELECT dst_id AS vertex_id, COUNT(1) AS in_degree FROM edges GROUP BY dst_id ORDER BY in_degree DESC;
	END IF;
	RETURN in_degree_stats;
END;
$$ LANGUAGE plpgsql STABLE;

/*
BEGIN TRANSACTION;  -- can only consume cursor within transction
SELECT get_in_degree_stats('in_degree_stats', FALSE); -- **single quotes**
FETCH ALL IN in_degree_stats;
COMMIT;
*/


-- get isolate vertices
CREATE OR REPLACE FUNCTION get_isolate_vertices(refcursor) RETURNS refcursor AS $$
DECLARE
	isolate_vertices ALIAS FOR $1;
BEGIN
	OPEN isolate_vertices FOR
	SELECT vertices.id  AS vertex_id FROM
	vertices LEFT JOIN
	(SELECT DISTINCT src_id FROM edges UNION SELECT DISTINCT dst_id FROM edges) AS t1
	ON vertices.id=t1.src_id WHERE t1.src_id IS NULL;
	RETURN isolate_vertices;
END;
$$ LANGUAGE plpgsql STABLE;

/*
BEGIN TRANSACTION;
SELECT get_isolate_vertices('isolate_vertices'); -- **single quotes**
FETCH ALL IN isolate_vertices;
COMMIT;
*/


-- get zero in/out degree vertices
CREATE OR REPLACE FUNCTION get_zero_degree_vertices(refcursor, integer) RETURNS refcursor AS $$
DECLARE
	zero_degree_vertices ALIAS FOR $1;
	orientation ALIAS FOR $2;
BEGIN
	IF orientation = 0 THEN
	    OPEN zero_degree_vertices FOR
	    SELECT t1.src_id AS vertex_id FROM
		(SELECT DISTINCT src_id FROM edges) AS t1 LEFT JOIN
		(SELECT DISTINCT dst_id FROM edges) AS t2
		ON t1.src_id = t2.dst_id WHERE t2.dst_id IS NULL;
	ELSIF orientation = 1 THEN
	    OPEN zero_degree_vertices FOR
	    SELECT t1.dst_id AS vertex_id FROM
		(SELECT DISTINCT dst_id FROM edges) AS t1 LEFT JOIN
		(SELECT DISTINCT src_id FROM edges) AS t2
		ON t1.dst_id = t2.src_id WHERE t2.src_id IS NULL;
	ELSE
		RAISE EXCEPTION 'ILLEGAL VALUE FOR `orientation`: %', orientation;
	END IF;
	RETURN zero_degree_vertices;
END;
$$ LANGUAGE plpgsql STABLE;

/*
BEGIN TRANSACTION;
SELECT get_zero_degree_vertices('zero_degree_vertices', 0); -- **single quotes**
FETCH ALL IN zero_degree_vertices;
END TRANSACTION;
*/