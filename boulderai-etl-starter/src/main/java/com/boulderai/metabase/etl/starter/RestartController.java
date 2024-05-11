package com.boulderai.metabase.etl.starter;

import com.boulderai.metabase.etl.e.engine.PgExtractEngine;
import com.boulderai.metabase.etl.starter.electMasterService.MasterLockService;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RestartController {

    @Autowired
    private PgExtractEngine pgExtractEngine;

    @Autowired
    private MasterLockService commonLockService;

    @GetMapping("/helloRestart")
    public String hello()
    {
        return "Hello Restart"+ new DateTime().toString();
    }

    @GetMapping("/reStart")
    public String reStart()
    {
        return DataEtlStarter.restart();
    }

    @GetMapping("/closeEngine")
    public Boolean closeEngine()
    {
        pgExtractEngine.closeEngine();
        return true;
    }

    @GetMapping("/createEngine")
    public Boolean createEngine() {
        pgExtractEngine.create();
        return true;
    }

    @GetMapping("/tryMaster")
    public Boolean tryMaster()
    {
        return commonLockService.lock();
    }

    @GetMapping("/abandonMaster")
    public Boolean abandonMaster() {
        return commonLockService.unlock();
    }

    @GetMapping("/forceUnlock")
    public Boolean forceUnlock() {
        return commonLockService.forceUnlock();
    }

    @GetMapping("/engineRun")
    public Boolean engineRun() {
        return pgExtractEngine.isRun();
    }

    @GetMapping("/seeMasterLock")
    public String seeMasterLock() {
        return commonLockService.getMasterLock();
    }
}
