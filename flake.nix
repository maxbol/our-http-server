{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    nixpkgs,
    flake-utils,
    self,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (system: let
      pkgs = import nixpkgs {inherit system;};
    in
      with pkgs; rec {
        packages.our-http-server = stdenv.mkDerivation {
          name = "our-http-server";
          version = "git";

          buildInputs = [
            clang
            gnumake
            pkg-config
            openssl
          ];
        };

        packages.default = packages.our-http-server;

        devShells.default = mkShell {
          packages = [
            clang-tools
            bear
            liburing
            clang-manpages
          ];

          inputsFrom = [
            self.packages.${system}.default
          ];
        };
      });
}
