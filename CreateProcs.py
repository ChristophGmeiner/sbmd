class CreateProcs:

    createproc1 = """
                 CREATE OR REPLACE PROCEDURE SP01_Update_w01live_city_names()

                    AS
                    $$
                    BEGIN

                    UPDATE t_w01_live
                    SET loc_name = 'Oberschleißheim'
                    WHERE loc_name = 'Oberschleissheim';

                    UPDATE t_w01_live
                    SET loc_name = 'Weßling'
                    WHERE loc_name = 'Wessling';

                    UPDATE t_w01_live
                    SET loc_name = 'Unterföhring'
                    WHERE loc_name IN ('Unterfoehring', 'Unterfohring');

                    UPDATE t_w01_live
                    SET loc_name = 'Türkenfeld'
                    WHERE loc_name IN ('Tuerkenfeld', 'Turkenfeld');

                    UPDATE t_w01_live
                    SET loc_name = 'Schöngeising'
                    WHERE loc_name IN ('Schoengeising', 'Schongeising');

                    UPDATE t_w01_live
                    SET loc_name = 'Röhrmoos'
                    WHERE loc_name IN ('Roehrmoos', 'Rohrmoos');

                    UPDATE t_w01_live
                    SET loc_name = 'Mühldorf am Inn'
                    WHERE loc_name IN ('Muehldorf am Inn', 'Muhldorf am Inn');

                    UPDATE t_w01_live
                    SET loc_name = 'Gröbenzell'
                    WHERE loc_name IN ('Groebenzell', 'Grobenzell');

                    UPDATE t_w01_live
                    SET loc_name = 'Gräfelfing'
                    WHERE loc_name IN ('Graefelfing', 'Grafelfing');

                    UPDATE t_w01_live
                    SET loc_name = 'Fürstenfeldbruck'
                    WHERE loc_name IN ('Fuerstenfeldbruck', 'Furstenfeldbruck');

                    UPDATE t_w01_live
                    SET loc_name = 'Eichstätt'
                    WHERE loc_name IN ('Eichstaett', 'Eichstatt');

                    UPDATE t_w01_live
                    SET loc_name = 'Donauwörth'
                    WHERE loc_name IN ('Donauwoerth', 'Donauworth');

                    UPDATE t_w01_live
                    SET loc_name = 'Altomünster'
                    WHERE loc_name IN ('Altomuenster', 'Altomunster');

                    UPDATE t_w01_live
                    SET loc_name = 'Unterschleißheim'
                    WHERE loc_name = 'Unterschleissheim';

                    UPDATE t_w01_live
                    SET loc_name = 'München'
                    WHERE loc_name = 'Munich';

                    UPDATE t_w01_live
                    SET loc_name = 'Nürnberg'
                    WHERE loc_name = 'Nuremberg';

                    UPDATE t_w01_live
                    SET loc_name = 'Kempten'
                    WHERE loc_name LIKE 'Kempten%';

                    END;
                    $$
                    LANGUAGE plpgsql
                    ;
                  """
    