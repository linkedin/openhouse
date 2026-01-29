import fs from 'fs';
import path from 'path';

/**
 * Reads the bearer token from dummy.token file
 * Checks multiple locations in order:
 * 1. Local spectacle directory
 * 2. Common services resources
 * 3. Environment variable
 * @returns Bearer token string or empty string if file doesn't exist
 */
export function getBearerToken(): string {
  try {
    // Check environment variable for token file path first
    const envTokenPath = process.env.OPENHOUSE_AUTH_TOKEN_FILE;
    if (envTokenPath) {
      console.log(`Checking for token at: ${envTokenPath}`);
      if (fs.existsSync(envTokenPath)) {
        try {
          const token = fs.readFileSync(envTokenPath, 'utf-8').trim();
          if (token && token.length > 0) {
            console.log(`✓ Using bearer token from: ${envTokenPath} (length: ${token.length})`);
            return token;
          } else {
            console.warn(`Token file exists but is empty: ${envTokenPath}`);
          }
        } catch (readError) {
          console.error(`Error reading token file ${envTokenPath}:`, readError);
        }
      } else {
        console.warn(`Token file not found: ${envTokenPath}`);
      }
    }

    // Fallback to possible token file locations
    const tokenPaths = [
      '/opt/token/openhouse-jwt-token', // Sidecar deployment
      '/var/tmp/tokens/openhouse_token', // Standalone deployment
      path.join(process.cwd(), 'dummy.token'), // Local override in spectacle directory
      path.join(process.cwd(), '../common/src/main/resources/dummy.token'), // Common services (dev mode)
    ];

    console.log('Trying fallback token paths...');
    for (const tokenPath of tokenPaths) {
      if (fs.existsSync(tokenPath)) {
        try {
          const token = fs.readFileSync(tokenPath, 'utf-8').trim();
          if (token && token.length > 0) {
            console.log(`✓ Using bearer token from: ${tokenPath} (length: ${token.length})`);
            return token;
          }
        } catch (readError) {
          console.warn(`Could not read ${tokenPath}:`, readError);
        }
      }
    }

    // Fallback to environment variable
    if (process.env.BEARER_TOKEN) {
      console.log('Using bearer token from BEARER_TOKEN environment variable');
      return process.env.BEARER_TOKEN;
    }

    console.warn('⚠ No bearer token found. API requests may fail.');
    return '';
  } catch (error) {
    console.error('Error in getBearerToken:', error);
    return process.env.BEARER_TOKEN || '';
  }
}
