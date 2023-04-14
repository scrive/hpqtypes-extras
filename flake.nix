{
  description = "Dev environment template for <PROJECT>";

  inputs = {
    # Unofficial library of utilities for managing Nix Flakes.
    flake-utils.url = "github:numtide/flake-utils";

    # Nix package set
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, flake-utils, nixpkgs }:
    flake-utils.lib.eachSystem
    (with flake-utils.lib.system; [ x86_64-linux x86_64-darwin aarch64-darwin ])
    (system: {
      # A Haskell development environment with provided tooling
      devShells.default =
        let
          # The compiler version to use for development
          compiler-version = "ghc944";
          pkgs = nixpkgs.legacyPackages.${system};
          inherit (pkgs) lib;
          hpkgs = pkgs.haskell.packages.${compiler-version};
          # Haskell and shell tooling
          tools = with hpkgs; [
            haskell-language-server
            ghc
            cabal-install
            cabal-plan
            (pkgs.haskell.lib.dontCheck ghcid)
          ];
          # System libraries that need to be symlinked
          libraries = with pkgs; [ pkg-config postgresql_11 zlib ];
          libraryPath = "${lib.makeLibraryPath libraries}";
        in hpkgs.shellFor {
          name = "dev-shell";
          packages = p: [ ];
          withHoogle = false;
          buildInputs = tools ++ libraries;

          LD_LIBRARY_PATH = libraryPath;
          LIBRARY_PATH = libraryPath;
        };
    });
}
