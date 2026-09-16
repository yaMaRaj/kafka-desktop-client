export namespace model {
	
	export class BulkProduceParams {
	    topic: string;
	    numRecords: number;
	    recordSize: number;
	    throughput: number;
	    keyPrefix?: string;
	    valuePrefix?: string;
	
	    static createFrom(source: any = {}) {
	        return new BulkProduceParams(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.topic = source["topic"];
	        this.numRecords = source["numRecords"];
	        this.recordSize = source["recordSize"];
	        this.throughput = source["throughput"];
	        this.keyPrefix = source["keyPrefix"];
	        this.valuePrefix = source["valuePrefix"];
	    }
	}
	export class HostAdminConfig {
	    host?: string;
	    port?: number;
	    username: string;
	    password?: string;
	    keyPath?: string;
	    skipHostKey: boolean;
	    configPath: string;
	    restartCommand: string;
	    statusCommand?: string;
	
	    static createFrom(source: any = {}) {
	        return new HostAdminConfig(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.host = source["host"];
	        this.port = source["port"];
	        this.username = source["username"];
	        this.password = source["password"];
	        this.keyPath = source["keyPath"];
	        this.skipHostKey = source["skipHostKey"];
	        this.configPath = source["configPath"];
	        this.restartCommand = source["restartCommand"];
	        this.statusCommand = source["statusCommand"];
	    }
	}
	export class SslConfig {
	    caPath?: string;
	    certPath?: string;
	    keyPath?: string;
	    passphrase?: string;
	    rejectUnauthorized?: boolean;
	
	    static createFrom(source: any = {}) {
	        return new SslConfig(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.caPath = source["caPath"];
	        this.certPath = source["certPath"];
	        this.keyPath = source["keyPath"];
	        this.passphrase = source["passphrase"];
	        this.rejectUnauthorized = source["rejectUnauthorized"];
	    }
	}
	export class SaslConfig {
	    mechanism: string;
	    username: string;
	    password: string;
	
	    static createFrom(source: any = {}) {
	        return new SaslConfig(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.mechanism = source["mechanism"];
	        this.username = source["username"];
	        this.password = source["password"];
	    }
	}
	export class ConnectionProfile {
	    id: string;
	    name: string;
	    bootstrapServers: string;
	    securityProtocol: string;
	    clientId?: string;
	    sasl?: SaslConfig;
	    ssl?: SslConfig;
	    schemaRegistryUrl?: string;
	    hostAdmin?: HostAdminConfig;
	    createdAt: number;
	    updatedAt: number;
	
	    static createFrom(source: any = {}) {
	        return new ConnectionProfile(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.name = source["name"];
	        this.bootstrapServers = source["bootstrapServers"];
	        this.securityProtocol = source["securityProtocol"];
	        this.clientId = source["clientId"];
	        this.sasl = this.convertValues(source["sasl"], SaslConfig);
	        this.ssl = this.convertValues(source["ssl"], SslConfig);
	        this.schemaRegistryUrl = source["schemaRegistryUrl"];
	        this.hostAdmin = this.convertValues(source["hostAdmin"], HostAdminConfig);
	        this.createdAt = source["createdAt"];
	        this.updatedAt = source["updatedAt"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class FetchMessagesParams {
	    topic: string;
	    partition?: number;
	    fromOffset?: number;
	    fromTimestamp?: number;
	    limit?: number;
	
	    static createFrom(source: any = {}) {
	        return new FetchMessagesParams(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.topic = source["topic"];
	        this.partition = source["partition"];
	        this.fromOffset = source["fromOffset"];
	        this.fromTimestamp = source["fromTimestamp"];
	        this.limit = source["limit"];
	    }
	}
	
	export class ProduceMessageParams {
	    topic: string;
	    key?: string;
	    value: string;
	    partition?: number;
	
	    static createFrom(source: any = {}) {
	        return new ProduceMessageParams(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.topic = source["topic"];
	        this.key = source["key"];
	        this.value = source["value"];
	        this.partition = source["partition"];
	    }
	}
	export class Result {
	    ok: boolean;
	    data?: any;
	    error?: string;
	
	    static createFrom(source: any = {}) {
	        return new Result(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.ok = source["ok"];
	        this.data = source["data"];
	        this.error = source["error"];
	    }
	}
	
	
	export class TopicCreateParams {
	    name: string;
	    numPartitions: number;
	    replicationFactor: number;
	
	    static createFrom(source: any = {}) {
	        return new TopicCreateParams(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.name = source["name"];
	        this.numPartitions = source["numPartitions"];
	        this.replicationFactor = source["replicationFactor"];
	    }
	}

}

