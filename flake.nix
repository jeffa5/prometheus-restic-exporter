{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
  };

  outputs = {
    self,
    nixpkgs,
  }: let
    system = "x86_64-linux";
    pkgs = nixpkgs.legacyPackages.${system};
    pre = pkgs.buildGoModule {
      pname = "prometheus-restic-exporter";
      version = "0.1.0";
      src = ./.;
      vendorHash = "sha256-b9CAU5kaFlAQskoCxzFGcEdwG6LCUhXZWFSzlzbNNPM=";
    };
  in {
    packages.${system} = {
      inherit pre;
      default = pre;
    };

    devShells.${system}.default = pkgs.mkShell {
      packages = [
        pkgs.go
        pkgs.gopls
      ];
    };
  };
}
