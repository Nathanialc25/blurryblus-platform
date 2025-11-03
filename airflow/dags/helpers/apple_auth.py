import jwt
import os
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class AppleAuthManager:
    """
    Manages Apple Music API JWT authentication.
    Returns a valid JWT token, generating one if needed.
    """
    
    def __init__(self, team_id: str, key_id: str, private_key_path: str, jwt_store_path: str):
        self.team_id = team_id
        self.key_id = key_id
        self.private_key_path = private_key_path
        self.jwt_store_path = jwt_store_path
    
    def _load_private_key(self) -> str:
        """Load the private key from file."""
        with open(self.private_key_path, 'r') as f:
            return f.read().strip()
    
    def _is_token_valid(self) -> bool:
        """Check if stored token is still valid."""
        try:
            #check if the location is even valid
            if not os.path.exists(self.jwt_store_path):
                return False
            
            #if location is valid, now check the expiration, return if its still good
            with open(self.jwt_store_path, "r") as f:
                token, expiration_str = f.read().strip().split('\n')[:2]
            
            return datetime.fromisoformat(expiration_str) > datetime.utcnow()
        #if none of the above worked, give back a false    
        except Exception:
            return False
    
    def get_valid_token(self) -> str:
        """
        Get a valid JWT token - generates one if needed.
        """
        # Return existing token if valid
        if self._is_token_valid():
            with open(self.jwt_store_path, "r") as f:
                return f.read().split('\n')[0].strip()
        
        # Generate new token if is_token_valid is false
        issued_at = datetime.utcnow()
        expiration = issued_at + timedelta(days=179)
        
        token = jwt.encode(
            {
                "iss": self.team_id,
                "iat": int(issued_at.timestamp()),
                "exp": int(expiration.timestamp()),
            },
            self._load_private_key(),
            algorithm="ES256",
            headers={"alg": "ES256", "kid": self.key_id, "typ": "JWT"}
        )
        
        # Store token
        os.makedirs(os.path.dirname(self.jwt_store_path), exist_ok=True)
        with open(self.jwt_store_path, "w") as f:
            f.write(f"{token}\n{expiration.isoformat()}\n")
        
        logger.info("Generated new JWT valid until %s", expiration.isoformat())
        return token