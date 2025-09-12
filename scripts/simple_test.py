#!/usr/bin/env python3
"""
Simple connectivity test for services
"""
import psycopg2
import redis
import requests
import time

def test_postgres():
    """Test PostgreSQL connection"""
    try:
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="airflow",
            user="airflow",
            password="airflow"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        print("✅ PostgreSQL: Connection successful")
        return True
    except Exception as e:
        print(f"❌ PostgreSQL: Connection failed - {e}")
        return False

def test_redis():
    """Test Redis connection"""
    try:
        r = redis.Redis(host='redis', port=6379, db=0)
        r.ping()
        print("✅ Redis: Connection successful")
        return True
    except Exception as e:
        print(f"❌ Redis: Connection failed - {e}")
        return False

def test_spark():
    """Test Spark Master UI"""
    try:
        response = requests.get('http://spark:8080', timeout=5)
        if response.status_code == 200:
            print("✅ Spark: Master UI accessible")
            return True
        else:
            print(f"❌ Spark: Master UI returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Spark: Master UI not accessible - {e}")
        return False

def test_schema_registry():
    """Test Schema Registry"""
    try:
        response = requests.get('http://schema-registry:8081/subjects', timeout=5)
        if response.status_code == 200:
            print("✅ Schema Registry: API accessible")
            return True
        else:
            print(f"❌ Schema Registry: API returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Schema Registry: API not accessible - {e}")
        return False

def main():
    print("🔍 Testing service connectivity...")
    print("=" * 50)
    
    tests = [
        test_postgres,
        test_redis,
        test_spark,
        test_schema_registry
    ]
    
    results = []
    for test in tests:
        results.append(test())
        time.sleep(1)  # Small delay between tests
    
    print("=" * 50)
    successful = sum(results)
    total = len(results)
    print(f"📊 Results: {successful}/{total} services connected successfully")
    
    if successful == total:
        print("🎉 All services are connected and working!")
    else:
        print("⚠️  Some services have connectivity issues")

if __name__ == "__main__":
    main()
