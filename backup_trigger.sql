

CREATE TRIGGER tb_timbang2_after_insert
AFTER INSERT ON tb_timbang2
FOR EACH ROW
BEGIN
    INSERT INTO tb_timbang2_log (nourut1, plant_id, aksi)
    VALUES (NEW.nourut1, new.plant_id, 'INSERT')
    ON DUPLICATE KEY UPDATE aksi='INSERT', log_time=NOW();
end


CREATE TRIGGER tb_timbang2_after_insert
AFTER INSERT ON tb_timbang2
FOR EACH ROW
BEGIN
    INSERT INTO tb_timbang2_log (NOURUT1, PLANT_ID, AKSI, LOG_TIME)
    VALUES (NEW.NOURUT1, NEW.PLANT_ID, 'INSERT', NOW())
    ON DUPLICATE KEY UPDATE AKSI='INSERT', LOG_TIME=NOW();
END

DROP TRIGGER IF EXISTS tb_timbang2_after_insert;


CREATE TRIGGER tb_timbang2_after_update
AFTER UPDATE ON tb_timbang2
FOR EACH ROW
BEGIN
    INSERT INTO tb_timbang2_log (nourut1, plant_id, aksi)
    VALUES (NEW.nourut1, new.plant_id, 'UPDATE')
    ON DUPLICATE KEY UPDATE aksi='UPDATE', log_time=NOW();
end

CREATE TRIGGER tb_timbang2_after_update
AFTER UPDATE ON tb_timbang2
FOR EACH ROW
BEGIN
    INSERT INTO tb_timbang2_log (NOURUT1, PLANT_ID, AKSI, LOG_TIME)
    VALUES (NEW.NOURUT1, NEW.PLANT_ID, 'UPDATE', NOW())
    ON DUPLICATE KEY UPDATE AKSI='UPDATE', LOG_TIME=NOW();
end

DROP TRIGGER IF EXISTS tb_timbang2_after_update;


CREATE TRIGGER tb_timbang2_after_delete
AFTER DELETE ON tb_timbang2
FOR EACH ROW
BEGIN
    INSERT INTO tb_timbang2_log (nourut1, plant_id, aksi)
    VALUES (OLD.nourut1, old.plant_id, 'DELETE')
    ON DUPLICATE KEY UPDATE aksi='DELETE', log_time=NOW();
end

CREATE TRIGGER tb_timbang2_after_delete
AFTER DELETE ON tb_timbang2
FOR EACH ROW
BEGIN
    INSERT INTO tb_timbang2_log (NOURUT1, PLANT_ID, AKSI, LOG_TIME)
    VALUES (OLD.NOURUT1, OLD.PLANT_ID, 'DELETE', NOW())
    ON DUPLICATE KEY UPDATE AKSI='DELETE', LOG_TIME=NOW();
END

DROP TRIGGER IF EXISTS tb_timbang2_after_delete;
-- DELETE FROM tb_timbang2 where nourut1 = '25030612SA124'; 
