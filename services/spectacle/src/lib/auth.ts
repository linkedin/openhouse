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
    // Possible token file locations (in order of priority)
    const tokenPaths = [
      path.join(process.cwd(), 'dummy.token'), // Local override in spectacle directory
      path.join(process.cwd(), '../common/src/main/resources/dummy.token'), // Common services (dev mode)
    ];
    
    // Try each path
    for (const tokenPath of tokenPaths) {
      if (fs.existsSync(tokenPath)) {
        const token = fs.readFileSync(tokenPath, 'utf-8').trim();
        if (token) {
          console.log(`Using bearer token from: ${tokenPath}`);
          return token;
        }
      }
    }
    
    // Fallback to environment variable
    if (process.env.BEARER_TOKEN) {
      console.log('Using bearer token from environment variable');
      return process.env.BEARER_TOKEN;
    }
    
    console.warn('No bearer token found. API requests may fail.');
    return '';
  } catch (error) {
    console.error('Error reading bearer token:', error);
    return process.env.BEARER_TOKEN || '';
  }
}
